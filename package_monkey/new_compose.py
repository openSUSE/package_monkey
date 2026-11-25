##################################################################
#
# New, simplified composer implementation
#
##################################################################

from .filter import Classification
from .util import debugmsg, infomsg, warnmsg, errormsg
from .util import loggingFacade
from .arch import ArchSet
from .packages import PackageCollection

__names__ = [
	'COMPOSE_UNSPEC',
	'COMPOSE_EXCLUDE',
	'COMPOSE_INCLUDE',
	'Composable',
	'ComposableRegistry',
	'CompositionRules',
	'CompositionBuilder',
	'ReasonRequires',
	'ReasonPolicy',
	'ReasonUnresolvable',
	'ReasonDisableArchitectures',
]

COMPOSE_UNSPEC = 0
COMPOSE_EXCLUDE = 1
COMPOSE_INCLUDE = 3

class Constraints(object):
	def __init__(self, label = None):
		self.label = label
		# FIXME: initialize architectures from label?
		self.validArchitectures = None
		self.validClasses = None
		self.closureRules = None

		self.supportStatement = None

	def __str__(self):
		attrs = []
		if self.validArchitectures is not None:
			attrs.append(f"arch=<{self.validArchitectures}>")
		if self.validClasses is not None:
			attrs.append(f"classes=<{self.validClasses}>")
		if self.closureRules is not None:
			attrs.append(f"closure={self.closureRules}")
		if not attrs:
			return "unconstrained"
		return ";".join(attrs)

	def __eq__(self, other):
		if self.validArchitectures != other.validArchitectures or \
		   self.validClasses != other.validClasses or \
		   self.closureRules != other.closureRules or \
		   self.supportStatement is not other.supportStatement:
			return False

		return True

	def copy(self):
		result = self.__class__()
		if self.validArchitectures is not None:
			result.validArchitectures = self.validArchitectures.copy()

		if self.validClasses is not None:
			result.validClasses = self.validClasses.copy()

		result.supportStatement = self.supportStatement
		result.closureRules = self.closureRules
		return result

	def enableArchitecture(self, arch):
		if self.validArchitectures is None:
			self.validArchitectures = ArchSet()
		self.validArchitectures.enable(arch)

	def disableArchitecture(self, arch):
		if self.validArchitectures is not None:
			self.validArchitectures.disable(arch)

	def constrainArchitectures(self, archSet):
		if self.validArchitectures is None:
			self.validArchitectures = archSet.copy()
		elif not self.validArchitectures.issubset(archSet):
			self.validArchitectures = self.validArchitectures.intersection(archSet)

	def enableClass(self, label):
		if self.validClasses is None:
			self.validClasses = Classification.createLabelSet()
		self.validClasses.add(label)

	def disableClass(self, label):
		if self.validClasses is not None:
			self.validClasses.discard(label)

	def isValidClass(self, klass):
		if self.validClasses is None:
			return True
		return klass in self.validClasses

	def isRequestedClass(self, klass):
		if self.closureRules is None:
			return False
		return self.closureRules.isRequestedClass(klass)

	def refine(self, other):
		assert(other is not None)
		assert(isinstance(other, self.__class__))

		def intersectWithDefaults(a, b):
			if a is None:
				return b
			if b is None:
				return a
			return a.intersection(b)

		if self is other:
			return

		self.validArchitectures = intersectWithDefaults(self.validArchitectures, other.validArchitectures)
		self.validClasses = intersectWithDefaults(self.validClasses, other.validClasses)

		# closure rules are not refined successively; more specific settings just overwrite
		# less specific settings.
		if self.closureRules is None:
			self.closureRules = other.closureRules

		return self

	def findComplementing(self, candidate, included):
		if self.closureRules is None:
			return None
		return self.closureRules.findComplementing(candidate, included)

	def setSupportLevel(self, level, klass = None):
		if self.supportStatement is None:
			self.supportStatement = SupportStatement(self)
		else:
			self.supportStatement = self.supportStatement.unshare(self)
		self.supportStatement.set(level, klass)

	def updateSupportStatement(self, otherConstraints):
		otherStatement = otherConstraints.supportStatement
		if self.supportStatement is None:
			self.supportStatement = otherStatement
		elif self.supportStatement is otherStatement:
			pass
		else:
			self.supportStatement = self.supportStatement.unshare(self)
			self.supportStatement.update(otherStatement)

class SupportStatement(object):
	def __init__(self, owner = None):
		self.owner = owner
		self.level = None
		self.classLevels = None

	def __eq__(self, other):
		if other is None:
			return False
		if self.level != other.level:
			return False

		all = set(self.classLevels.keys()).union(set(other.classLevels.keys()))
		for klass in all:
			if self.classLevels.get(klass) is not other.classLevels.get(klass):
				return False

		return True

	def __str__(self):
		words = []
		if self.level:
			words.append(self.level)
		if self.classLevels is not None:
			for klass, level in sorted(self.classLevels.items(), key = str):
				words.append(f"{klass}={level}")
		return " ".join(words) or "<none>"

	def set(self, level, klass = None):
		if klass is None:
			self.level = level
		else:
			if self.classLevels is None:
				self.classLevels = {}
			self.classLevels[klass] = level

	def get(self, klass):
		return self.classLevels.get(klass) or self.level

	def update(self, other):
		if other.level:
			self.level = other.level
		if other.classLevels is not None:
			for klass, level in other.classLevels.items():
				self.set(klass, level)

	def unshare(self, owner):
		if self.owner is owner:
			return self
		return self.copy(owner)

	def copy(self, owner = None):
		result = self.__class__(owner)
		result.level = self.level
		result.classLevels = self.classLevels.copy()
		return result

class SupportSummary(object):
	def __init__(self):
		self.map = {}
	
	def add(self, rpm, level):
		self.map[rpm] = level

	def items(self):
		return self.map.items()

class Justification(object):
	def __init__(self, reason, other = None):
		self.reason = reason
		self.other = other

	def __str__(self):
		if self.other is None:
			return self.reason
		return f"{self.reason}: {self.other}"

	def follow(self):
		if isinstance(self.other, Composable):
			for j in self.other.justifications:
				yield self.other, j

	globalDict = {}

	@classmethod
	def create(klass, reason, other = None):
		key = (reason, other)
		j = klass.globalDict.get(key)
		if j is None:
			j = klass(reason, other)
			klass.globalDict[key] = j
		return j

def ReasonPolicy(policy):
	return Justification.create("set by policy", policy)

def ReasonEpicExcluded(epicControl):
	return Justification.create("epic excluded", epicControl)

def ReasonOptionExcluded(optionControl):
	return Justification.create("build option excluded", optionControl)

def ReasonOptionNotIncluded(optionControl):
	return Justification.create("depends on missing option", optionControl)

def ReasonChoiceExcluded(choiceControl):
	return Justification.create("choice excluded", choiceControl)

def ReasonChoiceIncluded(choiceControl):
	return Justification.create("choice included", choiceControl)

def ReasonInvalidClass(classLabel):
	return Justification.create(f"class {classLabel} is not allowed in this context")

def ReasonPropagate(other):
	return Justification.create(f"propagated from {other.TYPE}", other)

def ReasonRequiredBy(other):
	return Justification.create(f"is required by {other.TYPE}", other)

def ReasonRequires(other):
	return Justification.create(f"requires {other.TYPE}", other)

def ReasonUnresolvable(other):
	return Justification.create(f"unresolvable", other)

def ReasonDisableArchitectures(archSet, other):
	return Justification.create(f"disable {archSet} because it requires {other.TYPE}", other)

class ComposerReasoning(object):
	class ReasonChain(object):
		def __init__(self, key, name, decision):
			self.key = key
			self.name = name
			self.architectures = None
			self.decision = decision
			self.follow = []

		def __str__(self):
			return f"{self.key} ({self.decision})"

	def __init__(self):
		self._reason = {}

	def add(self, composable, trace = False):
		key = f"{composable.TYPE}/{composable}"

		chain = self._reason.get(key)
		if chain is not None:
			return chain

		chain = self.ReasonChain(key, str(composable), composable.decisionString)
		self._reason[key] = chain

		if composable.trace:
			trace = True

		if isinstance(composable, Composable):
			if trace:
				infomsg(f"{chain.name} {chain.decision}:")
				for justification in composable.justifications:
					infomsg(f"   {justification} {justification.other}")

			for justification in composable.justifications:
				chase = None
				if justification.other is not None:
					with loggingFacade.temporaryIndent():
						chase = self.add(justification.other, trace)

				chain.follow.append((f"{justification}", chase))

			chain.architectures = composable._validArchitectures

		return chain

	def overrideInclude(self, rpmName, overrideArch):
		key = f"rpm/{rpmName}"

		oldChain = self._reason.get(key)
		chain = self.ReasonChain(key, rpmName, "include override")
		self._reason[key] = chain

		if oldChain is not None:
			chain.follow.append((f"was: {oldChain.name} ({oldChain.decision})", oldChain))
		if overrideArch:
			chain.architectures = overrideArch

	def overrideExclude(self, rpmName):
		key = f"rpm/{rpmName}"

		oldChain = self._reason.get(key)
		chain = self.ReasonChain(key, rpmName, "exclude override")
		self._reason[key] = chain

		if oldChain is not None:
			chain.follow.append((f"was: {oldChain.name} ({oldChain.decision})", oldChain))

	def getJustification(self, name, treeNode, verbose = False):
		chain = self._reason.get(f"rpm/{name}")
		if chain is None:
			treeNode.add(f"{name}: spurious decision")
			return

		if chain.architectures:
			node = treeNode.add(f"{name} {chain.decision} (arch {chain.architectures})")
		else:
			node = treeNode.add(f"{name} {chain.decision}")

		node.entry = chain

		self.follow(node, chain, [])

		if verbose:
			msgfunc = infomsg
		else:
			msgfunc = lambda s: None

		msgfunc(f"getJustification({name})")
		msgfunc(f"inspect {chain}")

		self.maybeCullRedundant(node, set(), msgfunc)

	def getJustificationWork(self, node, chain, seen):
		if chain in seen:
			return

		self.follow(node, chain, seen + [chain])

	def follow(self, node, entry, seen):
		for justification, otherEntry in entry.follow:
			tag = f"{justification}"
			if otherEntry is not None and \
			   otherEntry.architectures is not None and \
			   entry.architectures != otherEntry.architectures:
				tag += f" (arch {otherEntry.architectures})"
			child = node.add(f"{tag}")

			# preserve our reasoning entry inside the tree node;
			# used by the culling algorithm below
			child.entry = otherEntry

			if otherEntry is not None:
				self.getJustificationWork(child, otherEntry, seen)

	# This step tries to cull redundant reasons from the tree.
	# For instance, we frequently have cases where epic Foo is included,
	# and package A depends on B, C, and D all from epic Foo.
	# Without culling, the tree looks like this:
	#   A excluded
	#     because epic Foo excluded
	#     because it requires B, which is excluded
	#       because epic Foo excluded
	#     because it requires C, which is excluded
	#       because epic Foo excluded
	#       because it requires D, which is excluded
	#         because epic Foo excluded
	# Ideally, what we want to see is
	#   A excluded
	#     because epic Foo excluded
	def maybeCullRedundant(self, node, redundant, msgfunc):
		if not node.children:
			msgfunc(f"not culled because it has no children")
			return

		redundant = redundant.copy()
		inspect = []

		for name, child in sorted(node.children.items()):
			entry = child.entry
			if entry in redundant:
				child.culled = True
			else:
				child.culled = False
				if entry is not None:
					redundant.add(entry)
					inspect.append(child)

		msgfunc(f"{node.entry} has the following children:")
		for child in inspect:
			msgfunc(f" - {child.entry}")

		msgfunc(f"trying to cull redundant chains")
		with loggingFacade.temporaryIndent():
			for child in inspect:
				msgfunc(f"inspect {child.entry}")
				with loggingFacade.temporaryIndent():
					self.maybeCullRedundant(child, redundant, msgfunc)

			node.culled = True
			for name, child in list(node.children.items()):
				if child.culled:
					msgfunc(f"cull {name} {child.entry}")
					del node.children[name]
				else:
					node.culled = False

		if node.culled:
			msgfunc(f"result: {node.entry} can be culled; all its children are gone")

class Policy(object):
	TYPE = "policy"
	PERMITTED_CHILDREN = None

	@classmethod
	def class_init(klass):
		permitted = {}
		permitted[None] = set([Classification.TYPE_LAYER])
		permitted[Classification.TYPE_LAYER] = set([Classification.TYPE_EPIC])
		permitted[Classification.TYPE_EPIC] = set([Classification.TYPE_BUILD_OPTION, Classification.TYPE_AUTOFLAVOR])
		klass.PERMITTED_CHILDREN = permitted

	def __init__(self, label = None, constraints = None):
		if self.PERMITTED_CHILDREN is None:
			self.__class__.class_init()

		if label is None:
			self.name = "default"
			self.permitted = self.PERMITTED_CHILDREN[None]
			self.trace = False
		else:
			self.name = f"{label.type}:{label}"
			self.permitted = self.PERMITTED_CHILDREN.get(label.type)
			self.trace = label.trace

		self.label = label
		self.decision = COMPOSE_UNSPEC
		self.constraints = constraints or Constraints(label)
		self.justifications = set()

		self._children = {}

	def __str__(self):
		return self.name

	def __eq__(self, other):
		return self.decision == other.decision and self.constraints == other.constraints

	def __hash__(self):
		return hash(repr(self))

	def copy(self):
		result = self.__class__()
		result.name = self.name
		result.permitted = self.permitted
		result.decision = self.decision
		result.constraints = self.constraints.copy()
		result.trace = self.trace
		return result

	def createPolicy(self, label, decision = None):
		policy = self._children.get(label)
		if policy is None:
			if self.permitted is None or label.type not in self.permitted:
				raise Exception(f"Policy: cannot create a policy for {label.type}:{label} as child of {self}")

			policy = Policy(label, constraints = self.constraints)
			if label.type is Classification.TYPE_AUTOFLAVOR:
				policy.name = f"{self}+{label}"
			self._children[label] = policy

			# propagate our settings to the (more specific) child policy
			policy.refine(self)

		if decision is not None:
			policy.decision = decision

		return policy;

	def getChildPolicy(self, label):
		return self._children.get(label)

	@property
	def validArchitectures(self):
		if self.constraints is None:
			return None
		return self.constraints.validArchitectures

	@property
	def validClasses(self):
		if self.constraints is None:
			return None
		return self.constraints.validClasses

	# If the constraints we're using are shared with some other (less specific) label,
	# create a copy before modifying it.
	# Modifying constraints that are shared with a more specific label is OK, though
	def unshareConstraints(self):
		if self.constraints is None:
			self.constraints = Constaints(self.label)
		elif self.label is None or self.constraints.label is not self.label:
			self.constraints = self.constraints.copy()
			self.constraints.label = self.label

	def enableArchitecture(self, arch):
		if self._children:
			raise Exception(f"policy {self}: refusing to change architecture set after creating child policies")

		self.unshareConstraints()
		self.constraints.enableArchitecture(arch)

	def disableArchitecture(self, arch):
		if self._children:
			raise Exception(f"policy {self}: refusing to change architecture set after creating child policies")

		self.unshareConstraints()
		self.constraints.disableArchitecture(arch)

	def constrainArchitectures(self, archSet):
		if self.constraints is not None and \
		   self.constraints.validArchitectures.issubset(archSet):
			return

		self.unshareConstraints()

		if self.trace:
			infomsg(f"ARCH {self} {self.constraints.validArchitectures} constrained by {archSet}")
		self.constraints.constrainArchitectures(archSet)

		for child in self._children.values():
			if child.constraints is self.constraints:
				continue
			child.constrainArchitectures(self.constraints.validArchitectures)

	def clearClasses(self):
		if self._children:
			raise Exception(f"policy {self}: refusing to change valid rpm classes after creating child policies")

		if self.constraints is None:
			return

		self.unshareConstraints()
		self.constraints.validClasses = None

	def enableClass(self, label):
		if self._children:
			raise Exception(f"policy {self}: refusing to change valid rpm classes after creating child policies")
		assert(label.type is Classification.TYPE_CLASS)

		self.unshareConstraints()
		self.constraints.enableClass(label)

	def disableClass(self, label):
		if self._children:
			raise Exception(f"policy {self}: refusing to change valid rpm classes after creating child policies")
		assert(label.type is Classification.TYPE_CLASS)

		if self.constraints is None:
			return

		self.unshareConstraints()
		self.constraints.disableClass(label)

	def isValidClass(self, klass):
		if self.constraints is None:
			return True
		return self.constraints.isValidClass(klass)

	def isRequestedClass(self, klass):
		if self.constraints is None:
			return False
		return self.constraints.isRequestedClass(klass)

	# self is a policy assigned to a more specific label;
	# other is either the default policy, or was assigned to a less specific label
	def refine(self, other):
		assert(other is not None)
		assert(isinstance(other, self.__class__))

		if other.constraints is not None:
			self.unshareConstraints()
			self.constraints.refine(other.constraints)

		if other.trace:
			self.trace = True

		return self

	def propagateDecision(self, other):
		if self.decision == other.decision:
			self.justifications.add(ReasonPropagate(other))
			return False

		if self.decision != COMPOSE_UNSPEC:
			return False

		self.decision = other.decision

		self.justifications.add(ReasonPropagate(other))
		return True

	def propagateConstraints(self, constraints):
		self.unshareConstraints()
		self.constraints.refine(constraints)

	def refineAndPropagateDecision(self, other):
		changed = self.propagateDecision(other)
		return self.refine(other) or changed

	def findComplementing(self, candidate, included):
		if self.constraints is None:
			return None
		return self.constraints.findComplementing(candidate, included)

	@property
	def decisionString(self):
		return Policy.decisionAsString(self.decision)

	@staticmethod
	def decisionAsString(decision):
		if decision == COMPOSE_EXCLUDE:
			return "exclude"
		if decision == COMPOSE_INCLUDE:
			return "include"
		if decision == COMPOSE_UNSPEC:
			return "asneeded"
		return "???"

	def show(self):
		return self.policyString

	@property
	def policyString(self):
		verb = self.decisionAsString(self.decision)

		if self.decision != COMPOSE_EXCLUDE:
			if self.constraints is not None:
				verb += f";{self.constraints}"

		return verb

	def setSupportLevel(self, id, klass):
		self.constraints.setSupportLevel(id, klass)
		for child in self._children.values():
			child.updateSupportStatement(self)
	
	def updateSupportStatement(self, otherPolicy):
		self.constraints.updateSupportStatement(otherPolicy.constraints)

class ClosureRules(object):
	class ClassRule(object):
		def __init__(self, targetClass):
			self.targetClass = targetClass
			self.sourceClasses = Classification.createLabelSet()

		def add(self, sourceClass):
			self.sourceClasses.add(sourceClass)

	def __init__(self, name):
		self.name = name
		self.requestedClasses = None
		self._classRules = {}

	def __str__(self):
		return self.name

	def requestClass(self, klass):
		assert(klass.type is Classification.TYPE_CLASS)
		if self.requestedClasses is None:
			self.requestedClasses = Classification.createLabelSet()
		self.requestedClasses.add(klass)

	def isRequestedClass(self, klass):
		return self.requestedClasses is None or klass in self.requestedClasses

	def createClassRule(self, klass):
		classRule = self._classRules.get(klass)
		if classRule is None:
			classRule = self.ClassRule(klass)
			self._classRules[klass] = classRule
		return classRule

	# We can implement different approaches to closure here.
	#  - downward class closure: if we have selected an rpm with a certain class
	#    (libraries, man, i18n, ...) include all rpms of this build that have
	#    a 'lower' class.
	#    This implements rules like:
	#	if library: include runtime
	#	if apidoc: include api
	#  - class complementation: if we have selected an rpm with a certain class,
	#    include rpms with classes that complement the original rpm.
	#    This implements rules like:
	#	if library: include optimized build of library
	#	if library: include devel packages
	# For the time being, we just implement the latter. If the user wants the
	# former behavior, they can achieve that by specifying these as class rules.
	def findComplementing(self, candidate, included):
		classRule = self._classRules.get(candidate.rpmClass)
		if classRule is None:
			return None

		for rpmControl in included:
			if rpmControl.rpmClass in classRule.sourceClasses:
				# infomsg(f"MATCH {rpmControl} {rpmControl.rpmClass} -> {candidate} {candidate.rpmClass}")
				return rpmControl

		return None

class ComposableRegistry(object):
	composables = []

	@classmethod
	def add(klass, composable):
		klass.composables.append(composable)

	@classmethod
	def reset(klass):
		for composable in klass.composables:
			composable._decision = COMPOSE_UNSPEC
			composable._constraints = None
			composable._validArchitectures = None
			composable.justifications = set()

class Composable(object):
	def __init__(self):
		self.shippable = True
		self._decision = COMPOSE_UNSPEC
		self._constraints = None
		self._validArchitectures = None
		self.justifications = set()
		self.trace = False

		ComposableRegistry.add(self)

	def maybeTracePolicyUpdate(self):
		if self.trace:
			infomsg(f"POLICY: {self} update policy {self.policyString}")

	@property
	def policy(self):
		barf

	# returns true if there was a change potentially resulting in more
	# rpms to be added to the composition
	def setPolicy(self, policy):
		d = policy.decision
		if d < self._decision:
			return False

		if d > self._decision:
			self._constraints = policy.constraints
			self._decision = policy.decision
			self.justifications = set()
		elif self._constraints is None:
			self._constraints = policy.constraints
		elif self._constraints == policy.constraints:
			return False
		else:
			# refine the constraints
			self._constraints = self._constraints.copy()
			self._constraints.refine(policy.constraints)

		self.justifications.add(ReasonPolicy(policy))

		self.maybeTracePolicyUpdate()
		return True

	@property
	def decision(self):
		return self._decision

	@property
	def decisionString(self):
		return Policy.decisionAsString(self._decision)

	@property
	def constraints(self):
		return self._constraints

	@property
	def policyString(self):
		r = self.decisionString
		if self._constraints is not None:
			r += f";{self._constraints}"
		return r

	@property
	def isIncluded(self):
		return self.decision == COMPOSE_INCLUDE

	@property
	def isExcluded(self):
		return self.decision == COMPOSE_EXCLUDE

	def markExcluded(self, reason = None):
		if self.trace:
			infomsg(f"POLICY: {self} update policy exclude because {reason}")
		if self._decision == COMPOSE_INCLUDE:
			raise Exception(f"{self}: forbidden transition from included to excluded")
		self._decision = COMPOSE_EXCLUDE

		if reason is not None:
			if type(reason) is str:
				reason = Justification.create(reason)
			assert(isinstance(reason, Justification))
			self.justifications.add(reason)

	def propagateConstraints(self, constraints):
		if constraints is None:
			return False

		if not isinstance(constraints, Constraints):
			errormsg(f"propagateConstraints: bad type {type(constraints)}")
		assert(isinstance(constraints, Constraints))
		if self._constraints is None:
			self._constraints = constraints
		else:
			# unshare
			self._constraints = self._constraints.copy()
			self._constraints.refine(constraints)
		return True

	def propagateDecision(self, d, reason = None):
		if d < self._decision:
			raise Exception(f"{self}: refusing to change decision {self.decisionString} to {Policy.decisionAsString(d)}")

		if d > self._decision:
			# clear previous justifications
			self.justifications = set()

		if reason is not None:
			if type(reason) is str:
				reason = Justification.create(reason)
			assert(isinstance(reason, Justification))
			self.justifications.add(reason)

		if d > self._decision:
			self._decision = d
			self.maybeTracePolicyUpdate()

		return True

	def propagateDecisionFrom(self, other):
		reason = ReasonPropagate(other)
		return self.propagateDecision(other.decision, reason)

	def getSupportLevel(self, klass = None):
		cons = self.constraints
		if cons is None or cons.supportStatement is None:
			return None
		return cons.supportStatement.get(klass)

class CompositionRules(object):
	def __init__(self, builder):
		self.builder = builder
		self.classificationScheme = builder.classificationScheme
		self.architectures = ArchSet(('x86_64', 'ppc64le', 's390x', 'aarch64'))
		self.topicClasses = self.classificationScheme.allTopicClasses
		self.supportDictionary = None

		defaultConstraints = Constraints()
		defaultConstraints.validArchitectures = self.architectures
		defaultConstraints.validClasses = self.topicClasses.copy()
		defaultConstraints.closureRules = self.getClosureRules("default")

		self.defaultPolicy = Policy(constraints = defaultConstraints)
		self._rules = {}

		self.overrideRpms = {}

	def castToLabel(self, arg, labelType):
		return self.builder.castToLabel(arg, labelType)

	def createLayerPolicy(self, arg, parentPolicy = None):
		label = self.castToLabel(arg, Classification.TYPE_LAYER)
		return self.defaultPolicy.createPolicy(label)

	def createEpicPolicy(self, arg):
		label = self.castToLabel(arg, Classification.TYPE_EPIC)
		layerPolicy = self.createLayerPolicy(label.layer)
		return layerPolicy.createPolicy(label)

	def createOptionPolicy(self, arg):
		label = self.castToLabel(arg, Classification.TYPE_BUILD_OPTION)
		epicPolicy = self.createEpicPolicy(label.epic)
		return epicPolicy.createPolicy(label)

	def enableLayer(self, nameOrLabel):
		policy = self.createLayerPolicy(nameOrLabel)
		policy.decision = COMPOSE_INCLUDE

	def disableLayer(self, nameOrLabel):
		policy = self.createLayerPolicy(nameOrLabel)
		policy.decision = COMPOSE_EXCLUDE

	def enableEpic(self, nameOrLabel):
		policy = self.createEpicPolicy(nameOrLabel)
		policy.decision = COMPOSE_INCLUDE

	def disableEpic(self, nameOrLabel):
		policy = self.createEpicPolicy(nameOrLabel)
		policy.decision = COMPOSE_EXCLUDE

	def enableOption(self, nameOrLabel):
		policy = self.createOptionPolicy(nameOrLabel)
		policy.decision = COMPOSE_INCLUDE

	def disableOption(self, nameOrLabel):
		policy = self.createOptionPolicy(nameOrLabel)
		policy.decision = COMPOSE_EXCLUDE

	def enableFlavor(self, epicPolicy, flavorName):
		pass

	def disableFlavor(self, epicPolicy, flavorName):
		pass

	def enableClass(self, nameOrLabel, parentPolicy = None):
		if parentPolicy is None:
			parentPolicy = self.defaultPolicy
		label = self.castToLabel(nameOrLabel, Classification.TYPE_CLASS)
		return parentPolicy.enableClass(label)

	def disableClass(self, nameOrLabel, parentPolicy = None):
		if parentPolicy is None:
			parentPolicy = self.defaultPolicy
		label = self.castToLabel(nameOrLabel, Classification.TYPE_CLASS)
		return parentPolicy.disableClass(label)

	def getClosureRules(self, id):
		return self.builder.getClosureRules(id)

	def requestClass(self, nameOrLabel, closureRules):
		label = self.castToLabel(nameOrLabel, Classification.TYPE_CLASS)
		return closureRules.requestClass(label)

	def installSupportLevels(self, supportDictionary):
		self.supportDictionary = supportDictionary

	def translateSupportLevel(self, id):
		if self.supportDictionary is None:
			return id
		level = self.supportDictionary.get(id)
		if level is None:
			raise Exception(f"Unknown support level {id}")
		return level

	# helper function for building Justifications
	def optionsToControls(self, classificationResult, buildOptions):
		result = set()
		for optionLabel in buildOptions:
			result.add(classificationResult.addOption(buildOptions))
		return result

	def initializeEpicDefaults(self, epicControl, layerPolicy):
		epic = epicControl.label

		epicPolicy = layerPolicy.createPolicy(epic)
		epicPolicy.refine(layerPolicy)

		archSet = self.architectures.intersection(epic.architectures)
		epicPolicy.constrainArchitectures(archSet)

		if not epicControl.isExcluded:
			epicControl.setPolicy(epicPolicy)
		else:
			epicControl.propagateConstraints(epicPolicy.constraints)

	def excludeRpmsForEpic(self, classificationResult, epicControl):
		# If the epic is excluded, mark all its rpms as excluded by default.
		# Note that individual rpms may be included nevertheless, for example
		# if they belong to a build option (we may exclude LLVM19 but include
		# the llvm19_runtime build option).
		if epicControl.isExcluded:
			for rpmControl in epicControl.rpms:
				# Do not exclude the rpm if it is defined by an included option
				if rpmControl.definedByOption is not None:
					optionControl = rpmControl.definedByOption
					if optionControl.isIncluded:
						continue

				rpmControl.markExcluded(ReasonEpicExcluded(epicControl))
			return

		constraints = epicControl.constraints

		# exclude rpms based on their class
		for rpmControl in epicControl.rpms:
			if not constraints.isValidClass(rpmControl.rpm.new_class):
				rpmControl.markExcluded(ReasonInvalidClass(rpmControl.rpm.new_class))
			elif rpmControl.choice and rpmControl.choice.decision == COMPOSE_EXCLUDE:
				# This rpm is controlled by a flavor, such as Printing+funkyformat or
				# Zypper+syspython, and the flavor (or one of its underlying build options)
				# has been disabled.
				rpmControl.markExcluded(ReasonChoiceExcluded(rpmControl.choice))
			elif not rpmControl.allOptionsEnabled(self.validOptionLabels):
				for buildOption in rpmControl.optionSet.difference(self.validOptionLabels):
					optionControl = classificationResult.addOption(buildOption)
					rpmControl.markExcluded(ReasonOptionNotIncluded(optionControl))

	def collectRequiredPackages(self, classificationResult, rpmControl, architectures):
		if rpmControl.trace:
			infomsg(f"chase dependencies for {rpmControl}")

		rpmsToEnable = set()
		rpmsToEnable.add(rpmControl)

		queue = list(classificationResult.getRequired(rpmControl.rpm, architectures))
		while queue:
			req = queue.pop(0)

			if req in rpmsToEnable:
				continue

			if req.decision == COMPOSE_EXCLUDE:
				raise Exception(f"{rpmControl}: wanted to include this rpm but failed. It depends on {req}, which has been excluded")

			# FIXME: we should not stop descending if this would enable
			# additional architectures.
			if req.decision == COMPOSE_INCLUDE:
				continue

			queue += list(classificationResult.getRequired(req.rpm, architectures))
			rpmsToEnable.add(req)

		return rpmsToEnable

	def checkOptionAndEpic(self, epicControl, optionControl):
		if epicControl.decision == optionControl.decision:
			return True

		# If we have a decision on the epic, but the option is unspecified, just
		# propagate the decision from the epic to the option
		if optionControl.decision == COMPOSE_UNSPEC:
			optionControl.propagateDecision(epicControl.decision)
			return True

		# The other way round, if the option is enabled but the epic is unspecified,
		# do nothing. It's a perfectly valid config.
		if epicControl.decision == COMPOSE_UNSPEC:
			return True

		# We can exclude an epic, but mark one of its options enabled
		if epicControl.decision == COMPOSE_EXCLUDE and optionControl.decision == COMPOSE_INCLUDE:
			return True

		return False

	def tryIncludeRpm(self, rpmControl, becauseOf, classificationResult, trace = False):
		constraints = becauseOf.constraints

		trace = trace or rpmControl.trace

		if rpmControl.rpm.isUnresolvable:
			raise Exception(f"Refusing to include {rpmControl} ({rpmControl.decisionString})")

		if rpmControl.isExcluded:
			if trace:
				infomsg(f"{rpmControl} is excluded")
			if rpmControl.rpmClass == classificationResult.classificationScheme.importantClass:
				raise Exception(f"{rpmControl}: package marked as important, but it is excluded")
			return None

		if not constraints.isValidClass(rpmControl.rpmClass):
			if trace:
				infomsg(f"{rpmControl} class {rpmControl.rpmClass} not allowed here")
			rpmControl.markExcluded(ReasonInvalidClass(rpmControl.rpmClass))
			return None

		if not constraints.isRequestedClass(rpmControl.rpmClass):
			if trace:
				infomsg(f"{rpmControl} class {rpmControl.rpmClass} not requested")
			return None

		reason = None

		if rpmControl.choice:
			choiceControl = rpmControl.choice
			globalChoiceControl = choiceControl.globalFlavorControl

			if trace:
				infomsg(f"{rpmControl} depends on {choiceControl}; policy={choiceControl.policyString}")
				if choiceControl.isIncluded:
					infomsg(f"  included")
				infomsg(f"   global {choiceControl.globalFlavorControl} policy={choiceControl.globalFlavorControl.policyString}")

			# This rpm is controlled by a flavor, such as Printing+funkyformat or
			# Zypper+syspython. We have checked in excludeRpmsForEpic()
			# whether it has been excluded explicitly.
			if choiceControl.isIncluded:
				reason = ReasonChoiceIncluded(choiceControl)
			elif not choiceControl.autoEnableWhenPossible:
				if trace:
					infomsg(f"{rpmControl} depends on {choiceControl}; not enabled")
				return None

			assert(not globalChoiceControl.isExcluded)

			if globalChoiceControl.constraints is not None:
				constraints = constraints.copy()
				constraints.refine(globalChoiceControl.constraints)

		if reason is None:
			reason = ReasonPropagate(becauseOf)

		if trace and reason:
			infomsg(f"{rpmControl} is included because {reason}")

		# include the rpm if all of its required options are enabled.
		# rpms who do not depend on an options are included trivially.
		if not rpmControl.allOptionsEnabled(self.validOptionLabels):
			missing = rpmControl.optionSet.difference(self.validOptionLabels)
			raise Exception(f"{rpmControl} lacks options {missing}. Why didn't we disable it?")

		# chase package dependencies and include them
		closure = self.collectRequiredPackages(classificationResult, rpmControl, constraints.validArchitectures)
		assert(closure is not None)

		if rpmControl.setDecision(COMPOSE_INCLUDE, reason):
			rpmControl.propagateConstraints(constraints)

		reason = ReasonRequiredBy(rpmControl)
		for reqControl in closure:
			if reqControl is rpmControl:
				continue

			assert(not reqControl.rpm.isUnresolvable)

			if reqControl.setDecision(COMPOSE_INCLUDE, reason):
				# FIXME: how do we track the architectures for which we want
				# the dependency included?
				# reqControl.propagateConstraints(constraints)
				pass

		return closure

	def checkComplement(self, epicControl):
		constraints = epicControl.constraints

		for buildControl in epicControl.builds:
			candidates = set()
			included = set()

			trace = epicControl.trace or buildControl.trace

			for rpmControl in buildControl.rpms:
				if rpmControl.trace:
					trace = True

				# If an rpm is part of a build option, do not apply any
				# complementary rules
				if rpmControl.definedByOption:
					continue

				if rpmControl.decision == COMPOSE_INCLUDE:
					included.add(rpmControl)
				elif rpmControl.decision == COMPOSE_UNSPEC:
					# make double sure that the rpm's class is accepted in this context
					# (eg unresolved, 32bit, ...).
					assert(constraints.isValidClass(rpmControl.rpmClass))
					candidates.add(rpmControl)

			if not included:
				# none of the rpms belonging to this build have been selected
				if trace:
					infomsg(f"{epicControl}/{buildControl}: excluded")
				continue

			if not candidates:
				# all rpms belonging to this build have a verdict
				continue

			while candidates:
				retry = False
				for candControl in list(candidates):
					klass = candControl.rpmClass
					rpmControl = constraints.findComplementing(candControl, included)
					if rpmControl is not None:
						if trace or candControl.trace:
							infomsg(f"{candControl} complements {rpmControl}")
						candControl.propagateDecision(rpmControl.decision, ReasonPropagate(rpmControl))
						candidates.discard(candControl)
						retry = True

				if not retry:
					break

	def showAllTraced(self, classificationResult, msg):
		def onlyTraced(collection):
			return set(filter(lambda c: c.trace, collection))

		def showState(c, what):
			infomsg(f"{what} {c}: policy={c.policyString}")

		def showEpic(epic):
			epicControl = classificationResult.addEpic(epic)

			showState(epicControl, "epic")

			with loggingFacade.temporaryIndent():
				for optionControl in epicControl.definedOptions:
					showState(optionControl, "option")
					tracedOptions.discard(optionControl)
				for choiceControl in epicControl.choices:
					showState(choiceControl, "choice")

		tracedLayers = onlyTraced(classificationResult.layerLabels)
		tracedEpics = onlyTraced(classificationResult.epicLabels)
		tracedOptions = onlyTraced(classificationResult.options)
		tracedGlobalChoices = onlyTraced(classificationResult.globalChoices)

		if not (tracedEpics or tracedOptions or tracedGlobalChoices):
			return

		infomsg(f"STATE: {msg}")
		with loggingFacade.temporaryIndent():
			for layer in sorted(tracedLayers, key = str):
				policy = self.createLayerPolicy(layer)
				showState(policy, "layer")

				tracedLayerEpics = tracedEpics.intersection(layer.members)
				for epic in sorted(tracedLayerEpics, key = str):
					showEpic(epic)
				tracedEpics.difference_update(tracedLayerEpics)

			for epic in sorted(tracedEpics, key = str):
				showEpic(epic)

			if tracedOptions:
				infomsg("")
				for optionControl in tracedOptions:
					showState(optionControl, "option")

			if tracedGlobalChoices:
				infomsg("")
				for flavorControl in tracedGlobalChoices:
					showState(flavorControl, "global choice")

		infomsg("")

	def apply(self, classificationResult):
		ComposableRegistry.reset()

		defaultPolicy = self.defaultPolicy

		includedEpics = Classification.createLabelSet()

		# Propagate option decisions from config
		for buildOption in classificationResult.buildOptionLabels:
			optionControl = classificationResult.addOption(buildOption)

			optionPolicy = self.createOptionPolicy(buildOption)
			optionControl.setPolicy(optionPolicy)

			if optionControl.isExcluded:
				epicControl = optionControl.definedBy
				epicControl.markExcluded(ReasonOptionExcluded(optionControl))

		# propagate layer settings to epic, but do not propagate decisions yet.
		for layer in classificationResult.layerLabels:
			layerPolicy = self.createLayerPolicy(layer)
			for epic in layer.members:
				epicControl = classificationResult.addEpic(epic)
				self.initializeEpicDefaults(epicControl, layerPolicy)

		self.showAllTraced(classificationResult, "stage 1")

		# There may be epics that do not belong to a layer
		for epic in classificationResult.epicLabels:
			epicControl = classificationResult.addEpic(epic)
			if not epicControl.isExcluded:
				if epicControl.constraints is None:
					warnmsg(f"Epic {epic} not part of any layer")
					epicControl.setPolicy(defaultPolicy)
				assert(epicControl.constraints is not None)

			# Now push the policy bits to the options defined by this epic
			for optionControl in epicControl.definedOptions:
				if optionControl.isExcluded:
					assert(epicControl.isExcluded)
					continue

				optionControl.propagateConstraints(epicControl.constraints)

		# disable epics that depend on a disabled option
		# propagate layer decisions to epics, and epic decisions to build options
		for layer in classificationResult.layerLabels:
			layerPolicy = self.createLayerPolicy(layer)
			for epic in layer.members:
				epicControl = classificationResult.addEpic(epic)
				if epicControl.isExcluded:
					continue

				for buildOption in epic.requiredOptions:
					optionControl = classificationResult.addOption(buildOption)
					if optionControl.isExcluded:
						epicControl.markExcluded(ReasonOptionExcluded(optionControl))

				if epicControl.decision == COMPOSE_UNSPEC and \
				   layerPolicy.decision == COMPOSE_INCLUDE:
					if epicControl.trace:
						infomsg(f"enable {epicControl}")
					epicControl.propagateDecision(layerPolicy.decision, ReasonPropagate(layerPolicy))

				if epicControl.isIncluded:
					for buildOption in epic.requiredOptions:
						if epicControl.trace:
							infomsg(f"enable {buildOption}")
						optionControl = classificationResult.addOption(buildOption)
						optionControl.propagateDecision(epicControl.decision, ReasonPropagate(epicControl))

				# If SystemPython is included, we also mark option syspython as included, so that
				# all Epic+syspython packages get enabled automatically
				if epicControl.isIncluded:
					for optionControl in epicControl.definedOptions:
						optionControl.propagateDecision(epicControl.decision, ReasonPropagate(epicControl))

		self.showAllTraced(classificationResult, "after propagating layer settings to epics")

		for globalChoiceControl in classificationResult.globalChoices:
			if not globalChoiceControl.canInclude:
				for optionControl in globalChoiceControl.lackingOptions:
					globalChoiceControl.markExcluded(ReasonOptionNotIncluded(optionControl))
				assert(globalChoiceControl.decision == COMPOSE_EXCLUDE)
			elif globalChoiceControl.hasOptionDependencies:
				for optionControl in globalChoiceControl.requiredOptions:
					if optionControl.trace:
						infomsg(f"{globalChoiceControl} update {optionControl.policyString}")
					globalChoiceControl.propagateConstraints(optionControl.constraints)
					globalChoiceControl.propagateDecision(COMPOSE_INCLUDE, ReasonPropagate(optionControl))

		self.showAllTraced(classificationResult, "after propagating option settings to global choices")

		# Determine which options/flavors have been enabled explicitly
		for buildOption in classificationResult.buildOptionLabels:
			optionControl = classificationResult.addOption(buildOption)
			epicControl = optionControl.definedBy

			# The epic may be excluded, but one of its options may still be alive.
			if not epicControl.isExcluded \
			   and not self.checkOptionAndEpic(epicControl, optionControl):
				raise Exception(f"{epicControl}: build option {buildOption} policy {optionPolicy.show()} conflicts with epic policy {epicControl.policyString}")

		for epicControl in classificationResult.epics:
			for choiceControl in epicControl.choices:
				if epicControl.isExcluded:
					choiceControl.markExcluded(ReasonEpicExcluded(epicControl))
					continue

				if not choiceControl.canInclude:
					for optionControl in choiceControl.lackingOptions:
						choiceControl.markExcluded(ReasonOptionNotIncluded(optionControl))
					assert(choiceControl.decision == COMPOSE_EXCLUDE)
					continue

				# set/refine policy
				decision = COMPOSE_UNSPEC

				autoFlavor = choiceControl.globalFlavorControl.label
				epicPolicy = self.createEpicPolicy(epicControl.label)
				localChoicePolicy = epicPolicy.getChildPolicy(autoFlavor)
				if decision == COMPOSE_UNSPEC and localChoicePolicy is not None:
					decision = localChoicePolicy.decision

				if decision == COMPOSE_UNSPEC:
					decision = choiceControl.globalFlavorControl.decision

				if decision == COMPOSE_UNSPEC and choiceControl.autoEnableWhenPossible:
					decision = epicControl.decision

				choiceControl.propagateConstraints(choiceControl.globalFlavorControl.constraints)
				choiceControl.propagateDecision(decision)

				if decision == COMPOSE_INCLUDE:
					choiceControl.propagateConstraints(choiceControl.globalFlavorControl.constraints)
					if localChoicePolicy is not None:
						choiceControl.propagateConstraints(localChoicePolicy.constraints)

				if epicControl.trace:
					infomsg(f"Inspecting choice {choiceControl}")
					infomsg(f"   global {choiceControl.globalFlavorControl} policy={choiceControl.globalFlavorControl.policyString} label={choiceControl.globalFlavorControl.label}")
					if localChoicePolicy is not None:
						infomsg(f"   local policy={localChoicePolicy} decision={localChoicePolicy.show()}")
					infomsg(f"   resulting policy={choiceControl.policyString}")

				del localChoicePolicy
				del epicPolicy

		self.showAllTraced(classificationResult, "after propagating decisions to local choices")

		self.validOptionLabels = Classification.createLabelSet()
		for optionControl in classificationResult.options:
			if optionControl.isIncluded:
				self.validOptionLabels.add(optionControl.label)
			elif optionControl.isExcluded:
				for rpm in optionControl.rpms:
					rpm.markExcluded(ReasonOptionExcluded(optionControl))

		# if an option has been disabled, its rpms should be marked as
		# excluded. Then, we should walk _up_ the dependency chain and mark all
		# packages that depend on them as excluded as well
		for flavorControl in classificationResult.globalChoices:
			if flavorControl.decision == COMPOSE_INCLUDE:
				# explicitly included
				if not flavorControl.canInclude:
					# consistency problem
					fail
				continue

			if not flavorControl.hasOptionDependencies:
				# Do not auto-enable flavors that don't depend on an option
				pass
			elif flavorControl.canInclude:
				# loop over all options it requires, and constrain this flavor
				# according to the policies attached to these options.
				# For example, if the llvm17_runtime option is constrained
				# to non s390x architectures, then any flavor(s) that depend
				# on it should be arch-constrained as well.
				flavorPolicy = self.defaultPolicy.copy()
				for optionControl in flavorControl.requiredOptions:
					flavorControl.propagateConstraints(optionControl.constraints)
				flavorControl.propagateDecision(COMPOSE_INCLUDE, "all options are enabled")
			else:
				for optionControl in flavorControl.lackingOptions:
					flavorControl.markExcluded(ReasonOptionNotIncluded(optionControl))

		for epicControl in classificationResult.epics:
			self.excludeRpmsForEpic(classificationResult, epicControl)

		self.showAllTraced(classificationResult, "after excluding rpms")

		# Now that we've applied all exclusions to the respective RPMs, we
		# can go and mark the included ones (and their dependencies).

		for optionControl in classificationResult.options:
			if not optionControl.isIncluded:
				continue
			for rpmControl in optionControl.rpms:
				self.tryIncludeRpm(rpmControl, optionControl, classificationResult)

		self.showAllTraced(classificationResult, "after including rpms for options")

		# Loop over all included epics and select rpms.
		for epicControl in classificationResult.epics:
			if not epicControl.isIncluded:
				if epicControl.trace:
					infomsg(f"EPIC {epicControl} EXCLUDED decision={epicControl.decisionString}")
					self.showAllTraced(classificationResult, "here")
				continue

			if epicControl.trace:
				infomsg(f"EPIC {epicControl} {epicControl.policyString}")

			if epicControl.constraints is None:
				errormsg(f"{epicControl} constraints=None")

			for rpmControl in epicControl.rpms:
				trace = epicControl.trace or rpmControl.trace
				self.tryIncludeRpm(rpmControl, epicControl, classificationResult, trace)

		epicOrder = classificationResult.classificationScheme.epicOrder()
		for epic in epicOrder.topDownTraversal():
			epicControl = classificationResult.addEpic(epic)

			if epic.trace:
				infomsg(f"{epicControl}: policy={epicControl.policyString}")

			# If the user didn't specify a decision for this epic, we may or may not have
			# pulled in rpms from this epic due to dependencies.
			# If that is the case, consulte the closure rules to see whether we should pull
			# in additional rpms that complement what we have already
			# (eg libraries -> api; lib -> x86_64_v3, etc)
			if epicControl.decision == COMPOSE_UNSPEC:
				self.checkComplement(epicControl)

	def produceSolution(self, classificationResult):
		solution = PackageCollection()
		for epic in classificationResult.epics:
			for rpmControl in epic.rpms:
				if rpmControl.isIncluded:
					self.addResult(solution, rpmControl)

		return solution

	def produceSupportSummary(self, classificationResult):
		result = SupportSummary()
		for epic in classificationResult.epics:
			for rpmControl in epic.rpms:
				if rpmControl.rpm.isSynthetic:
					continue
				if rpmControl.isIncluded:
					level = rpmControl.getSupportLevel()
					if level is not None:
						result.add(rpmControl.rpm.name, level)

		return result

	def resolveIncrementalEpic(self, epic, classificationResult):
		result = PackageCollection()

		epicControl = classificationResult.addEpic(epic)
		if not epicControl.isExcluded:
			if epicControl.decision == COMPOSE_UNSPEC:
				epicControl.propagateDecision(COMPOSE_INCLUDE, f"incremental epic {epic} requested by config")

			for rpmControl in epicControl.rpms:
				found = self.tryIncludeRpm(rpmControl, epicControl, classificationResult, epicControl.trace)
				for rpmControl in found or []:
					self.addResult(result, rpmControl)

		return result

	def addResult(self, result, rpmControl):
		rpm = rpmControl.rpm

		if rpmControl.trace:
			infomsg(f"Include {rpmControl}; architectures:")
			infomsg(f"   product: <{self.architectures}>")
			infomsg(f"   rpm:     <{rpm.architectures}>")
			if rpmControl.constraints:
				infomsg(f"   cons:    <{rpmControl.constraints.validArchitectures}>")
			if rpmControl._validArchitectures is not None:
				infomsg(f"   ctrl:    <{rpmControl._validArchitectures}>")

		archSet = self.architectures.intersection(rpm.architectures)
		if rpmControl.constraints is not None:
			archSet.intersection_update(rpmControl.constraints.validArchitectures)

		if rpmControl._validArchitectures is not None:
			# doesn't happen a lot, but it does happen
			archSet.intersection_update(rpmControl._validArchitectures)

		labelHints = rpm.labelHints
		if labelHints is not None:
			if labelHints.overrideArch:
				missing = labelHints.overrideArch.difference(rpm.architectures)
				if missing:
					warnmsg(f"{rpm}: you enabled architecture(s) {missing} but the rpm does not support it!")
				if rpmControl.trace:
					infomsg(f"   hints:   arch=<{labelHints.overrideArch}>")
				archSet = labelHints.overrideArch
			else:
				if labelHints.includeArch:
					if rpmControl.trace:
						infomsg(f"   hints:   arch+=<{labelHints.includeArch}>")
					archSet.update(labelHints.includeArch)
				if labelHints.excludeArch:
					if rpmControl.trace:
						infomsg(f"   hints:   arch-=<{labelHints.excludeArch}>")
					archSet.difference_update(labelHints.excludeArch)

		if rpmControl.trace:
			infomsg(f"   eff:     <{archSet}>")

		result.add(rpm, archSet)

	def produceReasoning(self, classificationResult):
		result = ComposerReasoning()

		for epic in classificationResult.epics:
			for rpmControl in epic.rpms:
				result.add(rpmControl)

		return result

class LoaderGlue(object):
	class ClassRule(object):
		def __init__(self, rules, policy, klass):
			self.rules = rules
			self.policy = policy
			self.klass = klass

		def include(self):
			self.policy.enableClass(self.klass)

		def exclude(self):
			self.policy.disableClass(self.klass)

		def setSupportLevel(self, value):
			value = self.rules.translateSupportLevel(value)
			self.policy.setSupportLevel(value, self.klass)

	class ClassFacade(object):
		def __init__(self, rules, policy):
			self.rules = rules
			self.policy = policy

			policy.clearClasses()

		def getRuleByName(self, id):
			# infomsg(f"getRuleByName({self.policy}, class={id})")
			klass = self.rules.castToLabel(id, Classification.TYPE_CLASS)
			return LoaderGlue.ClassRule(self.rules, self.policy, klass)

	class PolicyMediator(object):
		def __init__(self, rules, policy):
			self.rules = rules
			self.policy = policy

		def include(self):
			self.policy.decision = COMPOSE_INCLUDE

		def exclude(self):
			self.policy.decision = COMPOSE_EXCLUDE

		def resetDecision(self):
			self.policy.decision = COMPOSE_UNSPEC

		def setSupportLevel(self, value):
			self.policy.setSupportLevel(value)

		def setArchitectures(self, names):
			self.policy.constrainArchitectures(ArchSet(names))

		def setClosureRules(self, name):
			self.policy.constraints.closureRules = self.rules.getClosureRules(name)

		def getRuleCollection(self, key):
			# infomsg(f"getRuleCollection(policy={self.policy}, {key})")

			# class.*, classes:
			if key.startswith('class'):
				return LoaderGlue.ClassFacade(self.rules, self.policy)

			# epic.*, epics:
			if key.startswith('epic'):
				# FIXME: we should make sure that the epics referenced from here
				# are really part of this layer
				return LoaderGlue.EpicFacade(self.rules, self.policy)

			# extra.*, extras:
			if key.startswith('extra'):
				# FIXME: only valid within an epic
				return LoaderGlue.FlavorFacade(self.rules, self.policy)

			return None

	class LayerFacade(object):
		def __init__(self, rules, policy):
			self.rules = rules
			self.policy = policy

		def getRuleByName(self, id):
			# infomsg(f"getRuleByName({self.policy}, layer={id})")
			layer = self.rules.castToLabel(id, Classification.TYPE_LAYER)

			childPolicy = self.rules.createLayerPolicy(layer)
			return LoaderGlue.PolicyMediator(self.rules, childPolicy)

	class EpicFacade(object):
		def __init__(self, rules, policy):
			self.rules = rules
			self.policy = policy

		def getRuleByName(self, id):
			# infomsg(f"getRuleByName({self.policy}, epic={id})")
			epic = self.rules.castToLabel(id, Classification.TYPE_EPIC)

			if self.policy.label is None:
				warnmsg(f"obsolete: defining policy for epic {epic} outside of layer {epic.layer}")
			elif self.policy.label is not epic.layer:
				raise Exception(f"you're trying to define policy for epic {epic} inside {self.policy.label}; please use {epic.layer} instead")

			childPolicy = self.rules.createEpicPolicy(epic)
			return LoaderGlue.PolicyMediator(self.rules, childPolicy)

	class OptionFacade(object):
		def __init__(self, rules, policy):
			self.rules = rules
			self.policy = policy

		def getRuleByName(self, id):
			# infomsg(f"getRuleByName({self.policy}, option={id})")
			buildOption = self.rules.castToLabel(id, Classification.TYPE_BUILD_OPTION)

			childPolicy = self.rules.createOptionPolicy(buildOption)
			return LoaderGlue.PolicyMediator(self.rules, childPolicy)

	class FlavorFacade(object):
		def __init__(self, rules, policy):
			self.rules = rules
			self.policy = policy

		def getRuleByName(self, id):
			# infomsg(f"getRuleByName({self.policy}, flavor={id})")
			flavorLabel = self.rules.classificationScheme.nameToAutoFlavor(id)

			childPolicy = self.policy.createPolicy(flavorLabel)
			return LoaderGlue.PolicyMediator(self.rules, childPolicy)


class ProductSpec(object):
	TYPE_BASEPRODUCT = 'baseproduct'
	TYPE_EXTENSION = 'extension'

	def __init__(self, builder, name = None):
		self.builder = builder

		self.id = name
		self.name = name
		self.baseProduct = None
		self.classificationScheme = builder.classificationScheme
		self.type = self.TYPE_BASEPRODUCT
		self._architectures = ArchSet()
		self.contractNames = []
		self.obsComposeKey = None
		self.releasePackage = None
		self.releaseEpic = None
		self.okayToChangeArchitectures = True

		self._overrideRpmExclude = None
		self._overrideRpmInclude = None

		self.rules = CompositionRules(builder)
		assert(self.rules.defaultPolicy is not None)

	def __str__(self):
		return self.id

	def getRuleCollection(self, key):
		if key == 'classes':
			assert(self.okayToChangeArchitectures)
			return LoaderGlue.ClassFacade(self.rules, self.rules.defaultPolicy)

		# We pass the architecture set to the policy constructor, so you cannot
		# change architectures after this
		self.okayToChangeArchitectures = False

		if key == 'layers':
			return LoaderGlue.LayerFacade(self.rules, self.rules.defaultPolicy)

		if key == 'epics':
			return LoaderGlue.EpicFacade(self.rules, self.rules.defaultPolicy)

		if key == 'options':
			return LoaderGlue.OptionFacade(self.rules, self.rules.defaultPolicy)

		return None

	def overrideRpmInclude(self, yamlList):
		self._overrideRpmInclude = yamlList

	def overrideRpmExclude(self, yamlList):
		self._overrideRpmExclude = yamlList

	@property
	def architectures(self):
		return self._architectures

	@architectures.setter
	def architectures(self, names):
		archSet = ArchSet(names)

		# we process architectures early, then we come back here a second time,
		# which should be a no-op
		if self._architectures == archSet:
			return

		assert(self.okayToChangeArchitectures)
		self._architectures = archSet

		self.rules.defaultPolicy.constraints.validArchitectures = archSet

	def installDefaultClosureRules(self):
		rules = self.rules
		policy = rules.defaultPolicy
		policy.closureRules = ClosureRules("auto")

		for name in ('runtime', 'libraries', 'default', 'x86_64_v3', 'user', 'api', 'x86_64_v3_api', 'apidoc', 'doc', 'man', 'i18n', 'important'):
			rules.requestClass(name, policy.closureRules)

class CompositionBuilder(object):
	def __init__(self, classificationScheme):
		self.classificationScheme = classificationScheme

		self.release = None
		self._default = None
		self._products = {}
		self._closureRules = {}
		self._supportDictionary = classificationScheme.policy.supportDictionary

	def createProduct(self, id):
		product = self._products.get(id)
		if product is None:
			product = ProductSpec(self, id)
			self._products[id] = product

			if id == 'defaults':
				self._default = product

			product.installDefaultClosureRules()
			if self._supportDictionary:
				product.rules.installSupportLevels(self._supportDictionary)

		return product

	def lookupProduct(self, id):
		return self._products.get(id)

	@property
	def products(self):
		for product in self._products.values():
			if product.name == 'defaults':
				continue
			yield product

	def createClosureRule(self, name):
		result = self._closureRules.get(name)
		if result is None:
			result = ClosureRules(name)
			self._closureRules[name] = result
		return result

	def getClosureRules(self, id):
		return self._closureRules.get(id)

	def castToLabel(self, arg, labelType):
		if type(arg) is str:
			label = self.classificationScheme.getTypedLabel(arg, labelType)
			if label is None:
				raise Exception(f"Cannot find {labelType} \"{arg}\": no such label")
		elif isinstance(arg, Classification.Label):
			label = arg
		else:
			raise Exception(f"Invalid argument \"{arg}\" (type {type(arg)}")

		if label.type is not labelType:
			raise Exception(f"Cannot find {labelType} \"{arg}\": incompatible label type {label.type}")
		return label

	@classmethod
	def load(klass, classificationScheme, path):
		from .floader import CompositionLoader

		spec = klass(classificationScheme)

		loader = CompositionLoader()
		loader.load(spec, path)

		return spec

