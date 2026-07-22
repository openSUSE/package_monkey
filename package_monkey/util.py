#
# Miscellaneous utility classes
#
import time
import fnmatch
import locale
import os
import datetime
import shutil

##################################################################
# A simple class for batched processing
# You can use this when you have a long-running data processing loop
# and you do not want to lose all progress when you hit a bug during
# development.
##################################################################
class BatchedUpdate:
	def __init__(self, processingFunction = None, commitFunction = None, chunkSize = 20):
		self.processingFunction = processingFunction
		self.commitFunction = commitFunction
		self.chunkSize = chunkSize
		self.deferred = []

	def __del__(self):
		self.flush()

	def processedOne(self, object = None):
		self.deferred.append(object)
		if len(self.deferred) >= self.chunkSize:
			self.flush()

	def flush(self):
		if self.deferred:
			if self.processingFunction is not None:
				self.processingFunction(self.deferred)

			if self.commitFunction is not None:
				self.commitFunction()
			self.deferred = []

##################################################################
# Simple tool to detect cycles in a graph
#
# class TreeNode:
#	CYCLES = CycleDetector("tree node")
#
#	def __init__(self, label):
#		self.label = label
#
#	def traverse(self, visitor):
#		with self.CYCLES.protect(self.label) as guard:
#			for child in self.children:
#				self.traverse(visitor)
#			visitor.visit(self)
#
##################################################################
class CycleException(Exception):
	def __init__(self, msg, cycle):
		super().__init__(msg)
		self.cycle = cycle

class CycleDetector(object):
	class Ticket:
		def __init__(self, detector, key):
			self.detector = detector
			self.key = key
			self.valid = False

#		def __bool__(self):
#			return self.valid

		def __enter__(self):
			self.valid = self.detector.acquire(self.key)
			return self

		def __exit__(self, *args):
			if self.valid:
				self.detector.release(self.key)
				self.valid = False

		def drop(self):
			if self.valid:
				self.detector.release(self.key)
				self.valid = False

	def __init__(self, name):
		self.name = name
		self.chain = []

	def protect(self, key):
		return self.Ticket(self, key)

	def getCycle(self, key):
		i = self.chain.index(key)
		return self.chain[i:] + [key]

	def push(self, key):
		# for very deep trees, we would need something more efficient than a linear search.
		# O(n^2) is good enough for the shallow trees we're dealing with here.
		if key in self.chain:
			return False

		self.chain.append(key)
		return True

	def pop(self):
		if self.chain:
			return self.chain.pop()
		return None

	def acquire(self, key):
		if not self.push(key):
			cycle = self.getCycle(key)
			raise CycleException(f"Detected {self.name} loop in {' -> '.join(map(str, cycle))}", cycle)
		return True

	def release(self, key):
		if self.pop() != key:
			raise Exception(f"Out-of-sequence release of key {key} in Cycle detector")


class LoggingCycleDetector(CycleDetector):
	def __init__(self, *args):
		super().__init__(*args)
		self.cycles = []

	def acquire(self, key):
		# for very deep trees, we would need something more efficient than a linear search.
		# O(n^2) is good enough for the shallow trees we're dealing with here.
		if key in self.chain:
			i = self.chain.index(key)
			self.cycles.append(self.chain[i:] + [key])
			return False

		if len(self.chain) > 1000:
			raise Exception("Looks like a cycle but you ignored all warnings")

		self.chain.append(key)
		return True

##################################################################
#
# filter a collection of things by rank
# A rank of None indicates no ranking at all, and is "less than" any other rank
#
##################################################################
def filterRanking(items, getRank, isBetterThan):
	bestRank = None
	found = []
	for item in items:
		rank = getRank(item)
		if rank != bestRank:
			if rank is None:
				continue

			if type(rank) != int:
				raise Exception(f"ranking function returns non-integer value {rank}")

			if bestRank is not None and isBetterThan(bestRank, rank):
				continue

			bestRank = rank
			found = []
		found.append(item)
	
	return found

def filterLowestRanking(items, getRank):
	return filterRanking(items, getRank, int.__lt__)

def filterHighestRanking(items, getRank):
	return filterRanking(items, getRank, int.__gt__)

##################################################################
#
# shutil.copytree implementation that supports existing
# directories - mimics the behaviour of dirs_exist_ok=True
# that has been implemented in Python 3.8+
#
##################################################################
def mergetree(sourceDir, destDir, ignore=None, dirs_exist_ok=False):
	entries = list(os.scandir(sourceDir))
	ignored_files = ignore(
		os.fspath(sourceDir),
		[e.name for e in entries]
	) if ignore is not None else []
	
	os.makedirs(destDir, exist_ok=dirs_exist_ok)
	
	for entry in entries:
		if entry.name in ignored_files:
			continue
		
		destFile = os.path.join(destDir, entry.name)
		
		if entry.is_dir():
			mergetree(entry.path, destFile, ignore=ignore,
				dirs_exist_ok=dirs_exist_ok)
		else:
			shutil.copy2(entry.path, destFile)
	
	shutil.copystat(sourceDir, destDir)
	
	return destDir

##################################################################
#
# Utility class for execution timing
#
##################################################################
class ExecTimer:
	def __init__(self):
		self.t0 = time.time()

	@property
	def elapsed(self):
		return time.time() - self.t0

	def __str__(self):
		return f"{self.elapsed:.3} sec"

class TimedExecutionBlock(object):
	def __init__(self, desc):
		self.description = desc
		self.timer = None

	def __enter__(self):
		if self.timer is None:
			infomsg(self.description.capitalize())
			self.timer = ExecTimer()

	def __exit__(self, *args):
		if self.timer is not None:
			if self.timer.elapsed > 1:
				infomsg(f"Spent {self.timer} seconds {self.description}")
			self.timer = None

class LoggingExecTimer(ExecTimer):
	def __str__(self):
		elapsed = self.elapsed
		min = int(elapsed / 60)
		sec = int(elapsed) % 60
		return f"[{min:02}:{sec:02}]"

##################################################################
#
# Add a number of months to a datetime.date object
#
##################################################################
def relativeDate(date, delta, roundToEndOfMonth = False):
	try:
		delta = int(delta)
	except:
		return None

	try:
		# so datetime.timedelta does not support "months = N" right now
		delta = datetime.timedelta(months = delta)
	except:
		delta = datetime.timedelta(days = delta * 31)

	endDate = date + delta

	if roundToEndOfMonth:
		endDate = roundUpToEndOfMonth(endDate)

	return endDate

def roundUpToEndOfMonth(date):
	(y, m, d) = (date.year, date.month + 1, 1)
	if m > 12:
		y += 1
		m = 1

	date = datetime.date(y, m, d)
	return date - datetime.timedelta(days = 1)


##################################################################
#
# A simple progress tracker
#
##################################################################
class ThatsProgress:
	def __init__(self, total, withETA = False):
		self.count = 0
		self.total = total

		self.timer = None
		if withETA:
			self.timer = ExecTimer()

	@property
	def percent(self):
		if self.total == 0:
			return 100
		return 100.0 * self.count / self.total

	def __str__(self):
		return f"{self.percent:3.1f}%"

	@property
	def eta(self):
		if not self.timer:
			return None
		if not self.count:
			return 0

		elapsed = self.timer.elapsed
		secRemaining = int(elapsed / self.count * (self.total - self.count))

		minRemaining = int(secRemaining / 60)
		secRemaining %= 60
		if minRemaining == 0:
			return f"{secRemaining:02}s"

		hrsRemaining = int(minRemaining / 60)
		minRemaining %= 60
		if hrsRemaining == 0:
			return f"{minRemaining:02}:{secRemaining:02}"

		return f"{hrsRemaining:02}:{minRemaining:02}:{secRemaining:02}"

	def tick(self, incr = 1):
		self.count += incr

class SimpleQueue(object):
	def __init__(self, arg):
		self.totalCount = 0
		self.progressMeter = None
		self.inProgress = None
		self._queue = []

		if type(arg) is int:
			self.totalCount = arg
		else:
			if type(arg) is list:
				self._queue = arg.copy()
			else:
				self._queue = list(arg)
			self.totalCount = len(self._queue)

	def append(self, item):
		self._queue.append(item)

	def __len__(self):
		return len(self._queue)

	def __bool__(self):
		return bool(self._queue)

	def __iter__(self):
		if self.progressMeter is None:
			self.progressMeter = ThatsProgress(len(self), withETA = True)

		while self._queue:
			yield self.next()

		self.done()

	def next(self):
		if self.inProgress:
			self.done()

		if not self._queue:
			return None

		item = self._queue.pop(0)
		self.inProgress = item
		return item

	def done(self):
		if not self.inProgress:
			return

		self.progressMeter.tick()
		self.inProgress = None

		if len(self) == 0:
			infomsg("Completed.")
		else:
			infomsg(f"{self.progressMeter} complete, {self.progressMeter.eta} remaining")

##################################################################
#
# A simple name matches
#
##################################################################
class NameMatcher:
	class ExactMatch(object):
		def __init__(self, pattern):
			self.pattern = pattern
			self.hit = False

		def match(self, name):
			return self.pattern == name

	class ShellMatch(object):
		def __init__(self, pattern):
			self.pattern = pattern
			self.hit = False

		def match(self, name):
			return fnmatch.fnmatchcase(name, self.pattern)

	def __init__(self, names = []):
		self.matches = []
		for name in names:
			self.add(name)

	def __len__(self):
		return len(self.matches)

	def add(self, name):
		if '*' in name or '?' in name or '[' in name:
			m = self.ShellMatch(name)
		else:
			m = self.ExactMatch(name)
		self.matches.append(m)

	def match(self, candidate):
		for m in self.matches:
			if m.match(candidate):
				m.hit = True
				return True

		return False

	def reportUnmatched(self):
		result = []
		for m in self.matches:
			if not m.hit:
				result.append(m.pattern)
		return result

##################################################################
# Simple helper classes
##################################################################
class CountingDict(object):
	def __init__(self):
		self._count = {}

	def increment(self, key, count):
		try:
			self._count[key] += count
		except:
			self._count[key] = count

	def __getitem__(self, key):
		return self._count.get(key, 0)

class UniqueList(object):
	def __init__(self, members = None):
		self._inorder = []
		self._set = set()

		if members is not None:
			self.update(members)

	def append(self, item):
		if item not in self._set:
			self._inorder.append(item)
			self._set.add(item)

	def update(self, members):
		for item in members:
			self.append(item)

	def __len__(self):
		return len(self._inorder)

	def __getitem__(self, index):
		return self._inorder[index]

	def __iadd__(self, members):
		for item in members:
			self.append(item)
		return self

	def __iter__(self):
		# Do not simply return iter(self._inorder)
		# The list may actually grow while we're iterating over it, and we want
		# to be able to deal with this.
		i = 0
		while i < len(self._inorder):
			yield self._inorder[i]
			i += 1

	def __str__(self):
		return f"[{', '.join(map(str, self._inorder))}]"

	def asList(self):
		return self._inorder

class DictOfSets(object):
	def __init__(self, setClass = set, returnCopy = False):
		self._dict = dict()
		self._setClass = setClass
		self._returnCopy = returnCopy
		self.modified = False

	def __bool__(self):
		return any(self._dict.values())

	def __len__(self):
		return len(self._dict)

	def __getitem__(self, key):
		return self.get(key)

	def __iter__(self):
		raise Exception(f"not iterable")

	def items(self):
		return self._dict.items()

	def add(self, key, value):
		setValue = self._dict.get(key)
		if setValue is None:
			setValue = self._setClass()
			self._dict[key] = setValue
		setValue.add(value)

		self.modified = True

	def update(self, key, values):
		try:
			self._dict[key].update(values)
		except:
			self._dict[key] = values.copy()

		self.modified = True

	def subtract(self, key, values):
		if key in self._dict:
			self._dict[key].difference_update(values)
		self.modified = True

	def discard(self, key, value):
		setValue = self._dict.get(key)
		if setValue is not None:
			setValue.discard(value)
			self.modified = True

	def get(self, key):
		setValue = self._dict.get(key)
		if setValue is None:
			return self._setClass()
		if self._returnCopy:
			return setValue.copy()
		return setValue

	def keys(self):
		return self._dict.keys()

	def values(self):
		return self._dict.values()

	def clear(self):
		self._dict.clear()

##################################################################
#
# Format tuples of (tag1, .. tagN, message) so that
# recurring tags are hidden, like a bibliographic index
#
##################################################################
class IndexFormatter(object):
	def __init__(self, msgfunc = print, sort = False):
		self.print = msgfunc
		self.sort = sort
		self.queue = []
		self.tags = []

	def __del__(self):
		self.flush()

	def __bool__(self):
		return bool(self.queue)

	def flush(self):
		if self.queue:
			for entry in sorted(self.queue):
				self.render(entry)
			self.print("")
			self.queue = []

	def next(self, *entry):
		entry = list(map(str, entry))
		if self.sort:
			self.queue.append(entry)
		else:
			self.render(entry)

	def render(self, entry):
		if not entry:
			return

		message = entry[-1]
		tags = entry[:-1]

		i = 0
		for a, b in zip(tags, self.tags):
			if a != b:
				break
			i += 1

		indent = i * "   "
		for tag in tags[i:]:
			indent += "   "
			self.print(f"{indent}{tag}")

		self.print(f"{indent} - {message}")
		self.tags = tags


##################################################################
# Format a tree of objects
##################################################################
class TreeFormatter(object):
	LINE_DOWN = ' |'
	TEE_RIGHT = ' +->'
	ARROW_DOWN_RIGHT = ' \\->'
	NO_LINE = '  '
	HSPACE = ' '

	class Node(object):
		def __init__(self, value):
			self.value = value
			self.children = {}

		def add(self, value):
			name = str(value)
			child = self.children.get(name)
			if child is None:
				child = self.__class__(value)
				self.children[name] = child
			return child

	def __init__(self):
		self.root = self.Node(None)

	def render(self):
		return self.renderWork(self.root)

	def renderWork(self, node, prefix = '', seen = None):
		if seen is None:
			seen = set()
		seen.add(node)

		queue = sorted(node.children.items())

		prefix += ' '
		cc0 = self.TEE_RIGHT
		cc1 = self.LINE_DOWN
		while queue:
			name, child = queue.pop(0)
			if not queue:
				cc0 = self.ARROW_DOWN_RIGHT
				cc1 = self.NO_LINE

			yield (prefix + cc0 + self.HSPACE, child.value)

			# There's a cycle in the graph
			if child in seen:
				continue

			for pair in self.renderWork(child, prefix + cc1 + self.HSPACE):
				yield pair

	def standout(self, s):
		return s

class ANSITreeFormatter(TreeFormatter):
	LINE_DOWN = ' \u2502'
	TEE_RIGHT = ' \u251C\u2500>'
	ARROW_DOWN_RIGHT = ' \u2514\u2500>'
	NO_LINE = '  '
	HSPACE = ' '

	RED = '\u001b[31m'
	YELLOW = '\u001b[33m'
	NOCOL = '\u001b[0m'

	def standout(self, s):
		return self.YELLOW + str(s) + self.NOCOL

##################################################################
# Tabular display
##################################################################
class TableFormatter(object):
	class Row(dict):
		def __init__(self, name, keys):
			self.name = name
			for s in keys:
				self[s] = '-'

		def __str__(self):
			return self.name

	def __init__(self, columns, fieldWidths = []):
		self.fieldWidths = fieldWidths
		self.columns = columns
		self.keys = list(map(str, columns[1:]))
		self.rows = []

		while len(self.fieldWidths) < len(self.columns):
			k = len(self.fieldWidths)
			self.fieldWidths.append(len(self.columns[k]) + 2)

	def addRow(self, name):
		r = self.Row(name, self.keys)
		self.rows.append(r)
		return r

	def render(self, header = None, displayfn = print):
		if not self.rows:
			return False

		def line(values):
			msg = ""
			for s, w in zip(values, self.fieldWidths):
				if msg:
					msg += " "
				msg += f"{s:{w}}"
			return msg

		if header is not None:
			displayfn(f"{header}")

		displayfn(line(self.columns))
		for r in self.rows:
			values = [r.name] + list(map(r.__getitem__, self.keys))
			displayfn(line(values))

		displayfn("")

##################################################################
# Expand "foo${variable}bar" strings
##################################################################
class VariableExpander(object):
	def __init__(self, defines = None):
		import re

		self.defines = defines or {}
		self.regex = re.compile('(.*)\${([^}]*)}(.*)')

	def update(self, key, value):
		self.defines[key] = value

	def expand(self, s):
		if '$' not in s:
			return s

		orig = s
		result = ''
		while True:
			m = self.regex.match(s)
			if not m:
				result += s
				break

			before, name, after = m.groups()
			replace = self.defines.get(name)
			if replace is None:
				warnmsg(f"{name} expands to nothing while performing variable expansion of \"{orig}\"")
				replace = ''

			result += before + str(replace)
			s = after


		debugmsg(f"variable expansion \"{orig}\" -> \"{result}\"")
		return result

##################################################################
#
# Base class for constructing simple templated classes
#
##################################################################
class Template(object):
	@classmethod
	def instantiate(klass, newKlassName, *args, **kwargs):
		members = kwargs.copy()
		for name, thing in klass.__dict__.items():
			if name in ('__str__', '__bool__', '__int__'):
				pass
			elif name.startswith('_') or thing is klass.instantiate:
				continue

			members[name] = thing

		members['__init__'] = klass.__init__
		members['super'] = lambda self: klass.Super(self, args[0])

		return type(newKlassName, args, members)

	class Super(object):
		def __init__(self, instance, baseClass):
			self.instance = instance
			self.baseClass = baseClass
			self.__init__ = lambda *args: baseClass.__init__(instance, *args)

class TemplateTest(object):
	class FancyInt(object):
		def __init__(self, value = 0):
			self.value = int(value)

		def display(self):
			print(f"{self.value} is a nice value")

	class FancyFloat(object):
		def __init__(self, value = 0):
			self.value = float(value)

		def display(self):
			print(f"{self.value} is a nice value")

	class Larval(Template):
		def __init__(self, *args):
			# Note: self.super() instead of super()!
			self.super().__init__(*args)

	@classmethod
	def run(klass):
		LarvalInt = klass.Larval.instantiate("Larval<int>", klass.FancyInt)
		LarvalFloat = klass.Larval.instantiate("Larval<float>", klass.FancyFloat)

		li = LarvalInt(42)
		li.display()

		lf = LarvalFloat(42)
		lf.display()

##################################################################
#
# Classes to detect how often some object is referenced (ie
# determining the object's frequency), and for detecting which
# objects have been referenced how often (extracting the
# objects that fall into given frequency bands).
#
##################################################################
class FrequencyCounter(object):
	def __init__(self, objectToKeyFunc):
		self.eventCount = {}
		self.totalEvents = 0
		self.objects = {}
		self.objectToKeyFunc = objectToKeyFunc

	def addEvent(self, objects):
		for obj in objects:
			key = self.objectToKeyFunc(obj)
			if key not in self.objects:
				self.objects[key] = obj
			try:
				self.eventCount[key] += 1
			except:
				self.eventCount[key] = 1

		self.totalEvents += 1

	def frequencyBands(self, thresholds):
		filter = MultiBandFrequencyFilter(self.totalEvents, thresholds)
		for key, eventCount in sorted(self.eventCount.items(), key = lambda item: -item[1]):
			obj = self.objects[key]
			filter.add(obj, eventCount)
		return filter

	def __iter__(self):
		for key, count in sorted(self.eventCount.items(), key = lambda pair: pair[1], reverse = True):
			yield self.objects[key], count

# That's a fancy class name, isn't it? :-)
class MultiBandFrequencyFilter:
	class Item:
		def __init__(self, object, freq, relativeFreq):
			self.object = object
			self.freq = freq
			self.relativeFreq = relativeFreq

	class FrequencyBand:
		def __init__(self, threshold):
			self.threshold = threshold
			self.items = []

		@property
		def objects(self):
			for i in self.items:
				yield i.object

	def __init__(self, total, thresholds):
		self.total = total
		self.bands = []
		for n in sorted(thresholds, reverse = True):
			self.bands.append(self.FrequencyBand(n))

	def add(self, object, eventCount):
		relativeFreq = int(100.0 * eventCount / self.total)
		package = self.Item(object, eventCount, relativeFreq)
		for b in self.bands:
			if relativeFreq >= b.threshold:
				b.items.append(package)
				return

##################################################################
# When printing eg a set of warnings, do not print anything
# when there are zero warnings; but print a caption if there
# is at least one warning
##################################################################
class OptionalCaption(object):
	def __init__(self, caption, msgfunc = None):
		self.caption = caption
		self.msgfunc = msgfunc or infomsg

	def __call__(self, msg = None):
		if self.caption is not None:
			self.msgfunc(self.caption)
			self.caption = None

		if msg is not None:
			self.msgfunc(f"   {msg}")

##################################################################
# Sort a list; trying to get the same result as sort(1) would give
##################################################################
class LocaleSetter(object):
	initialized = False

	@classmethod
	def init(klass):
		if not klass.initialized:
			localeName = os.environ.get("LC_ALL")
			if localeName is None:
				localeName = os.environ.get("LANG")
			if localeName is None:
				localeName = "C"
			locale.setlocale(locale.LC_ALL, localeName)

def locale_sorted(*args, key = None, **kwargs):
	LocaleSetter.init()

	if key is not None:
		new_key = lambda x: locale.strxfrm(key(x))
	else:
		new_key = locale.strxfrm
	return sorted(*args, key = new_key, **kwargs)

##################################################################
#
# Interfacing with python's logging class
#
##################################################################
import logging

class LoggingFacade:
	DEFAULT_FORMAT = '%(asctime)s: %(prefix)s%(message)s'
	NOTIME_FORMAT = '%(prefix)s%(message)s'

	class RelativeTimeFormatter(logging.Formatter):
		class Indent:
			def __init__(self):
				self.value = 0

			@property
			def whitespace(self):
				return " " * self.value

		class TI:
			def __init__(self, indent, width):
				self.indent = indent
				self.width = width
				self.active = False

			def __enter__(self):
				if not self.active:
					self.indent.value += self.width
					self.active = True
				return self

			def __exit__(self, *args):
				if self.active:
					self.indent.value -= self.width
					self.active = False

			def __del__(self):
				assert(not self.active)

		def __init__(self, *args, **kwargs):
			super().__init__(*args, **kwargs)
			self.timer = LoggingExecTimer()
			self.indent = self.Indent()

		def formatTime(self, record, datefmt = None):
			return str(self.timer)

		def format(self, record):
			if record.levelname == 'ERROR':
				record.prefix = "Error: "
			elif record.levelname == 'WARNING':
				record.prefix = "Warning: "
			else:
				record.prefix = self.indent.whitespace
			return super().format(record)

		def temporaryIndent(self, width = 3):
			return self.TI(self.indent, width)

	def __init__(self):
		self.root = logging.getLogger()
		self.root.setLevel(logging.INFO)

		self.default = self.getLogger('default')

		self.defaultFormat = self.RelativeTimeFormatter(self.DEFAULT_FORMAT)

	def addRootHandler(self, handler):
		handler.setFormatter(self.defaultFormat)
		self.root.addHandler(handler)

	def enableStdout(self):
		self.addRootHandler(logging.StreamHandler())

	def addLogfile(self, filename):
		self.addRootHandler(logging.FileHandler(filename, mode = "w"))

	def setLogLevel(self, name, levelName):
		levelName = levelName.upper()
		if name == 'all':
			logger = self.root
		else:
			logger = self.getLogger(name)

		logger.setLevel(levelName)
		logger.debug(f"Enabled {name} {levelName} messages")

	def isDebugEnabled(self, name):
		level = self.getLogger(name).getEffectiveLevel()
		return level <= logging.DEBUG

	def temporaryIndent(self, width = 3):
		return self.defaultFormat.temporaryIndent(width)

	def disableTimestamps(self):
		self.defaultFormat = self.RelativeTimeFormatter(self.NOTIME_FORMAT)
		# FIXME: this disables timestamps for all loggers; maybe we want to keep
		# them for logfiles and disable them for stdout only?
		for h in self.root.handlers:
			h.setFormatter(self.defaultFormat)

	def getLogger(self, name = None):
		return logging.getLogger(name)

loggingFacade = LoggingFacade()

debugmsg = loggingFacade.default.debug
infomsg = loggingFacade.default.info
warnmsg = loggingFacade.default.warning
errormsg = loggingFacade.default.error

