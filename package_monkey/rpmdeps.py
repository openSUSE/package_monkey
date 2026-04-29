##################################################################
# Processing of boolean dependencies
# 1) parse the expression from the RPM
# 2) create an intermediate presentation (can probably go away
#    again, at some point)
# 3) use the resolver to transform requirement strings to package names
# 4) break up complex dependencies into a sequence of simple
#    implications ("if X is installed, also install Y")
# 5) write those to codebase.db
# When composing the product, perform an additional validation step
# to ensure that these conditional dependencies are honored.
#
# Note: the syntax written to codebase.db is probably still too
# complex, and parsing it is also too complex. There should be
# something simpler (and more compact) like
#  A&B&!C=>D&E
# meaning if A, B and not C are installed, also install D and E
#  A&(B|C)=>(D|E)
# meaning if A is installed, as well as B or C, install either D or E.
##################################################################

from .util import infomsg, errormsg
import functools

__names__ = ['BooleanDependency']

class BooleanDependency(object):
	@classmethod
	def parse(klass, string, oracle):
		parser = DependencyParser(string, oracle)
		return parser.process()

	@classmethod
	def parseCompiled(klass, string):
		parser = NodeParser(string)
		node = parser.parse()
		if not parser.complete():
			raise Exception(f"parser did not consume entire string; remainder \"{parser.lexer.remainder}\"")
		return node

##################################################################
# Assertions represent a specific combination of included/excluded
# packages, resulting in a requirement
class Assertion(object):
	def __init__(self, include = [], exclude = [], implication = None, isFALSE = False):
		self.include = set(include)
		self.exclude = set(exclude)
		self.implication = implication
		self.isFALSE = isFALSE

	def __str__(self):
		result = ""
		if self.include:
			result += f"include({', '.join(map(str, self.include))})"
		if self.exclude:
			if result:
				result += "; "
			result += f"exclude({', '.join(map(str, self.exclude))})"
		if self.implication:
			result += f" => {self.implication}"
		return result

	def copy(self):
		return self.__class__(include = self.include, exclude = self.exclude, implication = self.implication)

	def negated(self):
		assert(self.implication is None)
		return self.__class__(include = self.exclude, exclude = self.include)

	def impliesAny(self, implications):
		for i in implications:
			if self.include.issubset(i.include) and \
			   not self.exclude.intersection(i.include) and \
			   not self.include.intersection(i.exclude):
				# infomsg(f"{self} implies {i}")
				return True
		return False

	@property
	def solutions(self):
		if self.implication is None:
			return []

		if isinstance(self.implication, AssertionAlternatives):
			return list(iter(self.implication))

		return [self.implication]

	def asNode(self):
		conditions = []
		for required in self.include:
			conditions.append(RequirementNode([required]))
		if self.exclude:
			conditions.append(NotNode(RequirementNode(self.exclude)))

		if len(conditions) == 0:
			return FailingNode()

		if len(conditions) == 1:
			result = conditions[0]
		else:
			result = AndNode(conditions)

		if self.implication is not None:
			result = ConditionalNode(result, self.implication.asNode())
		return result

	@classmethod
	def FALSE(klass):
		return klass(isFALSE = True)

	@classmethod
	def AND(klass, items):
		result = klass()
		for i in items:
			assert(i.implication is None)
			result.include.update(i.include)
			result.exclude.update(i.exclude)
		return result

	@classmethod
	def OR(klass, items):
		return AssertionAlternatives(items)

class AssertionAlternatives(object):
	def __init__(self, items):
		self.alternatives = list(items)

	def __iter__(self):
		return iter(self.alternatives)

	def __str__(self):
		return f"OR({', '.join(map(str, self.alternatives))})"

	def asNode(self):
		if len(self.alternatives) == 1:
			return self.alternatives[0].asNode()

		return OrNode(list(alt.asNode() for alt in self.alternatives))

##################################################################
class Node(object):
	def __init__(self):
		pass

class FailingNode(Node):
	def canEvaluateTrue(self):
		return False

	def __str__(self):
		return "FALSE"

	def permutations(self):
		yield Assertion.FALSE()

class FunctionCallNode(Node):
	def __init__(self, name):
		super().__init__()
		self.name = name

	def __str__(self):
		return self.name

	def canEvaluateTrue(self):
		return True

class RequirementNode(Node):
	def __init__(self, choices):
		super().__init__()
		self.choices = choices

	def __str__(self):
		if len(self.choices) == 1:
			rpmname, = self.choices
			return rpmname

		return f"or({', '.join(sorted(self.choices))})"

	def canEvaluateTrue(self):
		return bool(self.choices)

	def permutations(self):
		for rpmName in self.choices:
			yield Assertion(include = [rpmName])

class ConditionalNode(Node):
	def __init__(self, condition, thenClause, elseClause = None):
		super().__init__()
		assert(isinstance(condition, Node))
		assert(isinstance(thenClause, Node))
		assert(elseClause is None or isinstance(elseClause, Node))
		self.condition = condition
		self.thenClause = thenClause
		self.elseClause = elseClause

	def __str__(self):
		result = f"if({self.condition}, {self.thenClause}"
		if self.elseClause:
			result += f", {self.elseClause}"
		result += ")"
		return result

	# an if node can only evaluate to "False" iff:
	#	the condition can be true
	#	the then clause is never true
	#	an else clause exists and is never true
	def canEvaluateTrue(self):
		if not self.condition.canEvaluateTrue():
			# infomsg(f"{self}: condition {self.condition} is never true")
			return True

		if self.thenClause.canEvaluateTrue():
			# infomsg(f"{self}: then-clause {self.thenClause} can be true")
			return True

		if self.elseClause is not None and \
		   self.elseClause.canEvaluateTrue():
			# infomsg(f"{self}: else-clause {self.elseClause} can be true")
			return True

		return False

	def expandsToNothing(self):
		if not self.condition.canEvaluateTrue():
			if self.elseClause is None:
				# infomsg(f"{self}: condition {self.condition} is never true, and there's no else-clause")
				return True
			if not self.elseClause.canEvaluateTrue():
				# infomsg(f"{self}: condition {self.condition} is never true, and the else-clause is never true, either")
				return True
			return False

		if isinstance(self.condition, RequirementNode) and \
		   isinstance(self.thenClause, RequirementNode) and \
		   self.elseClause is None:
			conditionSet = set(self.condition.choices)
			thenSet = set(self.thenClause.choices)
			trivial = conditionSet.intersection(thenSet)
			if conditionSet.issubset(trivial):
				return True

		if self.thenClause.canEvaluateTrue():
			# infomsg(f"{self}: then-clause {self.thenClause} can be true")
			return False

		if self.elseClause is not None and \
		   self.elseClause.canEvaluateTrue():
			# infomsg(f"{self}: else-clause {self.elseClause} can be true")
			return False

		return True

	def permutations(self):
		if not self.condition.canEvaluateTrue():
			if self.elseClause and self.elseClause.canEvaluateTrue():
				yield Assertion.OR(self.elseClause.permutations())
			return

		thenImplications = Assertion.OR(list(self.thenClause.permutations()))
		if self.elseClause is not None:
			elseImplications = Assertion.OR(list(self.elseClause.permutations()))
		else:
			elseImplications = None

		for k in self.condition.permutations():
			if not k.include and not k.exclude:
				# This condition does not depend on packages; it is
				# just something like a macro call that can be true or false
				for i in self.thenClause.permutations():
					yield i
				if self.elseClause:
					for i in self.elseClause.permutations():
						yield i
			else:
				if k.implication:
					raise

				if not k.isFALSE and thenImplications and not k.impliesAny(thenImplications):
					ret = k.copy()
					ret.implication = thenImplications
					yield ret

				if elseImplications:
					implications = Assertion.OR(self.elseClause.permutations())
					ret = k.inverted()
					ret.implication = elseImplications
					yield ret

	def implications(self):
		for k in self.permutations():
			yield k.asNode()

class NotNode(Node):
	def __init__(self, condition):
		super().__init__()
		assert(isinstance(condition, Node))
		self.condition = condition

	def __str__(self):
		return f"not({self.condition})"

	# we can make any condition evaluate to false (by not installing what it asks for);
	# So we can make any "NOT" node evaluate to true.
	def canEvaluateTrue(self):
		return True

	def permutations(self):
		for assertion in self.condition.permutations():
			yield assertion.negated()

class AndNode(Node):
	def __init__(self, children):
		super().__init__()
		assert(all(isinstance(child, Node) for child in children))
		self.children = children

	def __str__(self):
		return f"and({', '.join(str(child) for child in self.children)})"

	def canEvaluateTrue(self):
		return all(child.canEvaluateTrue() for child in self.children)

	def permutations(self):
		result = Assertion()
		matrix = []
		for child in self.children:
			childPerms = list(child.permutations())
			if not childPerms:
				return
			matrix.append(childPerms)

		dim = list(map(len, matrix))
		iters = list(map(iter, matrix))
		current = list(map(next, iters))
		while True:
			yield Assertion.AND(current)
			pos = len(iters) - 1
			while pos >= 0:
				it = iters[pos]
				try:
					current[pos] = next(it)
					break
				except:
					pass
				iters[pos] = iter(matrix[pos])
				current[pos] = next(iters[pos])
				pos -= 1
			if pos < 0:
				break

class OrNode(Node):
	def __init__(self, children):
		super().__init__()
		assert(all(isinstance(child, Node) for child in children))

		self.children = []
		for child in children:
			if child.canEvaluateTrue():
				self.children.append(child)

	def __str__(self):
		return f"or({', '.join(str(child) for child in self.children)})"

	def canEvaluateTrue(self):
		return any(child.canEvaluateTrue() for child in self.children)

	def permutations(self):
		for child in self.children:
			for a in child.permutations():
				yield a

class ComparisonNode(Node):
	OPERAND = {
	'=':	'__eq__',
	'!=':	'__ne__',
	'<=':	'__le__',
	'>=':	'__ge__',
	'<':	'__lt__',
	'>':	'__gt__',
	}

	def __init__(self, name, *args):
		super().__init__()
		assert(isinstance(a, Node) for a in args)
		self.name = name
		self.children = args

	def __str__(self):
		return f"{self.name}({', '.join(str(child) for child in self.children)})"

	def permutations(self):
		yield Assertion()

class NodeParser(object):
	class Lexer(object):
		EOL		= 'EOL'
		IDENTIFIER	= 'IDENTIFIER'
		PUNCT		= 'PUNCT'

		def __init__(self, string):
			self.value = list(string)
			self.pos = 0

			self._saved = None

		def getc(self):
			try:
				cc = self.value[self.pos]
			except:
				return None

			self.pos += 1
			return cc

		def ungetc(self, cc):
			assert(self.pos > 0)
			assert(self.value[self.pos - 1] == cc)
			self.pos -= 1

		@property
		def remainder(self):
			return ''.join(self.value[self.pos:])

		def next(self):
			if self._saved is not None:
				token, value = self._saved
				self._saved = None
			else:
				token, value = self._next()
				# infomsg(f"## {token} {value}")

			return token, value

		def _next(self):
			result = []
			cc = self.getc()
			while cc and cc.isspace():
				cc = self.getc()

			if not cc:
				return (self.EOL, None)

			if cc.isalnum() or cc == '_':
				while cc and (cc.isalnum() or cc in "_.+-:"):
					result.append(cc)
					cc = self.getc()
				if cc is not None:
					self.ungetc(cc)
				token = self.IDENTIFIER
			else:
				result.append(cc)
				token = self.PUNCT
			return token, ''.join(result)

		def pushback(self, token, value):
			assert(self._saved is None)
			self._saved = (token, value)

		def unexpected(self, token, value):
			raise Exception(f"Unexpected token {token} (value=\"{value}\"); remaining string={self.remainder}")

	def __init__(self, string):
		# infomsg(f"PARSE {string}")
		self.lexer = self.Lexer(string)

	def parse(self):
		node = self.parseExpression()
		assert(not self.lexer.remainder)
		return node

	def complete(self):
		return not self.lexer.remainder

	def parseExpression(self):
		lexer = self.lexer

		token, value = lexer.next()
		if token == lexer.EOL:
			return None

		if token != lexer.IDENTIFIER:
			lexer.unexpected(token, value)

		identifier = value

		token, value = lexer.next()
		if value != '(':
			lexer.pushback(token, value)
			return self.nodeFromIdentifier(identifier)

		token, value = lexer.next()
		if value == ')':
			# empty call
			return self.functionCallNode(identifier)
		lexer.pushback(token, value)

		args = []
		while True:
			args.append(self.parseExpression())

			token, value = lexer.next()
			if value == ')':
				break;
			if value != ',':
				lexer.unexpected(token, value)

		return self.functionCallNode(identifier, args)

	@classmethod
	def nodeFromIdentifier(self, identifier):
		if identifier == 'FALSE':
			return FailingNode()

		return RequirementNode([identifier])

	@classmethod
	def functionCallNode(self, identifier, args = []):
		if identifier == 'if':
			return ConditionalNode(*args)
		if identifier in ComparisonNode.OPERAND.values() and len(args) == 2:
			return ComparisonNode(identifier, *args)
		if identifier == 'not' and len(args) == 1:
			return NotNode(*args)
		if identifier == 'or' and args:
			if all(isinstance(a, RequirementNode) for a in args):
				names = functools.reduce(set.union, (a.choices for a in args), set())
				return RequirementNode(names)
			return OrNode(args)
		if identifier == 'and' and args:
			return AndNode(args)
		if identifier in ('product-update',):
			expr = f"{identifier}({', '.join(map(str, args))})"
			return FunctionCallNode(expr)
		raise Exception(f"Don't know how to represent call {identifier}({', '.join(map(str, args))})")

##################################################################
class DependencyParser(object):
	class Lexer(object):
		EOL = 0
		LEFTB = 1
		RIGHTB = 2
		OPERATOR = 3
		IDENTIFIER = 4

		CHARCLASS_OPERATOR = ('<', '>', '=', '!')
		CHARCLASS_WORDBREAK = CHARCLASS_OPERATOR

		OPERATOR_IDENTIFIERS = ('EQ', 'NE', 'LT', 'GT', 'LE', 'GE')
		OPERATOR_TABLE = {
			'=':  '=',
			'==': '=',
			'<=': '<=',
			'>=': '>=',
			'<':  '<',
			'>':  '>',
			'!=': '!=',
		}


		def __init__(self, string):
			self.value = list(string)
			self.pos = 0

		def __str__(self):
			return "".join(self.value)

		def getc(self):
			try:
				cc = self.value[self.pos]
			except:
				return None

			self.pos += 1
			return cc

		def ungetc(self, cc):
			assert(self.pos > 0)
			assert(self.value[self.pos - 1] == cc)
			self.pos -= 1

		def next(self):
			result = ""
			while True:
				cc = self.getc()
				if cc is None:
					break

				while cc and cc.isspace():
					cc = self.getc()

				if cc in self.CHARCLASS_OPERATOR:
					while cc in self.CHARCLASS_OPERATOR:
						result += cc
						cc = self.getc()
					# translate operator "<=" to "LE" and so on
					result = self.OPERATOR_TABLE[result]
					return (self.OPERATOR, result)

				if cc == '(':
					return (self.LEFTB, cc)
				if cc == ')':
					return (self.RIGHTB, cc)

				processingBracketedArgument = False
				while cc and not cc.isspace() and not cc in self.CHARCLASS_WORDBREAK:
					if cc == '(':
						if processingBracketedArgument:
							raise Exception("Dependency parser: nested brackets not allowed inside Identifier")
						processingBracketedArgument = True
					elif cc == ')':
						if not processingBracketedArgument:
							break
						processingBracketedArgument = False

					result += cc
					cc = self.getc()

				if cc:
					self.ungetc(cc)

				if not result:
					break

				if result in self.OPERATOR_IDENTIFIERS:
					return (self.OPERATOR, result)

				return (self.IDENTIFIER, result)

			return (self.EOL, result)

		def symbolicToStringOperator(self, op):
			return self.OPERATOR_TABLE[op]

	class ProcessedExpression(object):
		pass

	class TrivialNodeWrapper(ProcessedExpression):
		def __init__(self, node):
			self.node = node

		def build(self):
			return self.node

	class AssociativeExpression(ProcessedExpression):
		def __init__(self, child):
			self.children = [child]

		def add(self, child):
			self.children.append(child)

	class OrExpression(AssociativeExpression):
		def build(self):
			return OrNode(self.children)

	class AndExpression(AssociativeExpression):
		def build(self):
			return AndNode(self.children)

	class WithExpression(AssociativeExpression):
		def build(self):
			condition = AndNode(self.children[1:])
			return ConditionalNode(condition, self.children[0])

	class WithoutExpression(AssociativeExpression):
		def build(self):
			condition = AndNode(self.children[1:])
			return ConditionalNode(NotNode(condition), self.children[0])

	def buildSingleton(self, name, flags = None, version = None):
		if self.oracle.isMacroInvocation(name):
			node = FunctionCallNode(name)
			if flags:
				op = ComparisonNode.OPERAND[flags]
				node = ComparisonNode(op, node, version)
			return node

		if flags:
			choices = self.oracle.whatprovides(name, flags, version)
		else:
			choices = self.oracle.whatprovides(name)

		choices = list(s.name for s in choices)
		if not choices:
			return FailingNode()
		return RequirementNode(choices)

	def __init__(self, string, oracle):
		self.lex = self.Lexer(string)
		self.lookahead = None
		self.oracle = oracle

	def __str__(self):
		return str(self.lex)

	def nextToken(self):
		lookahead = self.lookahead
		if lookahead is not None:
			self.lookahead = None
			return lookahead

		type, value = self.lex.next()
		# infomsg(f"## -> type={type} value=\"{value}\"")
		return type, value

	def pushBackToken(self, *args):
		assert(self.lookahead is None)
		self.lookahead = args

	class BadExpressionException(Exception):
		def __init__(self, lexer):
			value = "".join(lexer.value)
			ws = " " * lexer.pos
			msg = f"Bad expression:\n{value}\n{ws}^--- HERE"
			super().__init__(msg)

	def BadExpression(self):
		return self.BadExpressionException(self.lex)

	def process(self, endToken = None, deferElse = False):
		if endToken is None:
			endToken = self.Lexer.EOL

		leftTerm = None
		while True:
			type, value = self.nextToken()
			if type == endToken:
				break

			if type == self.Lexer.RIGHTB or type == self.Lexer.EOL:
				infomsg(f"endToken={endToken}")
				raise self.BadExpression()

			if type == self.Lexer.IDENTIFIER and value in ("if", "unless"):
				operator = value

				if leftTerm is None:
					raise self.BadExpression()

				condTerm = self.process(endToken, deferElse = True)
				thenTerm = leftTerm.build()
				elseTerm = None

				if self.lookahead == (self.Lexer.IDENTIFIER, "else"):
					self.lookahead = None
					elseTerm = self.process(endToken)

				if operator == "unless":
					condTerm = NotNode(condTerm)

				return ConditionalNode(condTerm, thenTerm, elseTerm)

			groupClass = None
			if type == self.Lexer.IDENTIFIER:
				if value == "or":
					groupClass = self.OrExpression
				elif value == "and":
					groupClass = self.AndExpression
				elif value == "with":
					groupClass = self.WithExpression
				elif value == "without":
					groupClass = self.WithoutExpression
				elif value == "else" and deferElse:
					self.pushBackToken(type, value)
					return leftTerm.build()

			if groupClass:
				if leftTerm is None:
					raise self.BadExpression()

				if not isinstance(leftTerm, self.AssociativeExpression):
					leftTerm = groupClass(leftTerm.build())
				elif leftTerm.__class__ != groupClass:
					infomsg("Cannot mix terms with different precendence")
					raise self.BadExpression()
				else:
					# process the next term and process it using leftTerm.add()
					pass

				type, value = self.nextToken()

			if type == self.Lexer.LEFTB:
				term = self.process(endToken = self.Lexer.RIGHTB)
			else:
				if type != self.Lexer.IDENTIFIER:
					raise self.BadExpression()

				args = [value]

				type, value = self.nextToken()
				if type == self.Lexer.OPERATOR:
					args.append(value)

					type, value = self.nextToken()
					if type != self.Lexer.IDENTIFIER:
						raise self.BadExpression()

					args.append(value)
				else:
					self.pushBackToken(type, value)

				term = self.buildSingleton(*args)

			if leftTerm:
				leftTerm.add(term)
			else:
				leftTerm = self.TrivialNodeWrapper(term)

		return leftTerm.build()
