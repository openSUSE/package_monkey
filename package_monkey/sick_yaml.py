##################################################################
#
# Very silly yaml formatter
#
##################################################################
from .util import infomsg, errormsg
import time

__names__ = ["YamlFormatter", "YamlDictProducerBase", "YamlMultiDictProducerBase"]

def _debugNone(*args, **kwargs):
	pass

def _debugMessage(*args, **kwargs):
	infomsg(*args, **kwargs)

debugScanner = _debugNone
debugParser = _debugMessage
debugData = _debugMessage

class YamlFormatter(object):
	class Element(object):
		def __init__(self, indent, writefn, firstEntryLead = None):
			self.indent = indent
			self.childIndent = indent + "  "
			self.firstEntryLead = firstEntryLead
			self.writefn = writefn

		def write(self, msg):
			if self.firstEntryLead is not None:
				self.writefn(self.firstEntryLead + msg)
				self.firstEntryLead = None
			else:
				self.writefn(self.indent + self.prefix + msg)

		def addSpacing(self):
			self.writefn("")

		def createDict(self):
			return YamlFormatter.Dict(self.childIndent + '  ', self.writefn)

		def createList(self):
			return YamlFormatter.List(self.childIndent, self.writefn)

		def addDict(self, name):
			self.write(f"{name}:")
			return self.createDict()

		def addString(self, name, value):
			if '\n' in value:
				self.write(f"{name}: |")
				for line in value.split('\n'):
					self.write(f"   {line}")
			else:
				self.write(f"{name}: {value}")

		def addComment(self, string):
			self.writefn(self.indent + "# " + string)

	class List(Element):
		def __init__(self, *args, **kwargs):
			super().__init__(*args, **kwargs)
			self.prefix = "- "
			self.childIndent = self.indent + len(self.prefix) * " "

		# FIXME: phase out
		def add(self, item):
			self.write(item)

		def addScalar(self, item):
			assert('\n' not in item)
			self.write(item)

		def createDict(self):
			return YamlFormatter.Dict(self.childIndent, self.writefn,
						firstEntryLead = self.indent + self.prefix)

		def addList(self):
			self.write("")
			return self.createList()

		def addDict(self):
			return self.createDict()

	class Dict(Element):
		def __init__(self, *args, **kwargs):
			super().__init__(*args, **kwargs)
			self.prefix = ""

		def addList(self, name):
			self.write(f"{name}:")
			return self.createList()

	def __init__(self, writefn):
		self.root = self.Dict("", writefn)

	def format(self, data):
		assert(type(data) is dict)
		self.formatDict(data, self.root, emptyLineAfter = True)

	def formatDict(self, data, parent, emptyLineAfter = False):
		for key, value in data.items():
			if type(value) is dict:
				self.formatDict(value, parent.addDict(key))
				if emptyLineAfter:
					parent.addSpacing()
			elif type(value) is list:
				self.formatList(value, parent.addList(key))
				if emptyLineAfter:
					parent.addSpacing()
			else:
				parent.addString(key, value)

	def formatList(self, data, parent):
		for item in data:
			if type(item) is dict:
				for key, value in item.items():
					if type(value) is dict:
						parent.add(f"{key}:")
						self.formatDict(value, parent.createDict())
						parent.addSpacing()
					elif type(value) is list:
						if all(type(_) is str for _ in value):
							parent.add(f"{key}: [{','.join(value)}]")
						else:
							parent.add(f"{key}:")
							self.formatList(value, parent.createList())
							parent.addSpacing()
					else:
						parent.add(f"{key}: {value}")
			elif type(item) is list:
				self.formatList(item, parent.createList())
			else:
				parent.add(item)

	def addDict(self, name):
		return self.root.addDict(name)

	def addList(self, name):
		return self.root.addList(name)

	def addSpacing(self):
		return self.root.addSpacing()

	def generateContent(self):
		return self.actualFile.generateContent()

##################################################################
# Format composer output as yaml file(s)
##################################################################
class YamlProducerBase(object):
	class NodeBase(object):
		pass

	class AbstractListNode(NodeBase):
		def __init__(self, addExtraSpacing = False):
			self.addExtraSpacing = addExtraSpacing

	class ListNode(AbstractListNode):
		def __init__(self, items = [], **kwargs):
			super().__init__(kwargs)
			self.items = [] + items

		def addEntry(self, value):
			self.items.append(value)
			return value

		def addDict(self):
			return self.addEntry(YamlProducerBase.DictNode())

		def addComment(self, comment):
			self.addEntry(YamlProducerBase.CommentNode(comment))

		def render(self, listFormatter):
			assert(isinstance(listFormatter, YamlFormatter.List))
			for value in self.items:
				if isinstance(value, YamlProducerBase.AbstractListNode):
					value.render(listFormatter.addList())
				elif isinstance(value, YamlProducerBase.AbstractComment):
					listFormatter.addComment(str(value))
				elif isinstance(value, YamlProducerBase.AbstractDictNode):
					value.render(listFormatter.addDict())
				else:
					listFormatter.addScalar(f"{value}")

				if self.addExtraSpacing:
					listFormatter.addSpacing()

	class AbstractDictNode(NodeBase):
		def __init__(self, addExtraSpacing = False):
			self.addExtraSpacing = addExtraSpacing

	class DictNode(AbstractDictNode):
		def __init__(self, **kwargs):
			super().__init__(kwargs)
			self.items = []

		def addEntry(self, name, value):
			self.items.append((name, value))
			return value

		def createList(self, name, values = []):
			return self.addEntry(name, YamlProducerBase.ListNode(values))

		def createDict(self, name):
			return self.addEntry(name, YamlProducerBase.DictNode())

		def createScalar(self, name, value):
			return self.addEntry(name, value)

		def addComment(self, comment):
			self.addEntry(None, YamlProducerBase.CommentNode(comment))

		def render(self, dictFormatter):
			assert(isinstance(dictFormatter, YamlFormatter.Dict))
			for name, value in self.items:
				if isinstance(value, YamlProducerBase.DictNode):
					value.render(dictFormatter.addDict(name))
				elif isinstance(value, YamlProducerBase.AbstractListNode):
					value.render(dictFormatter.addList(name))
				elif isinstance(value, YamlProducerBase.AbstractComment):
					dictFormatter.addComment(str(value))
				elif type(value) in (str, int, float, bool):
					dictFormatter.addString(name, value)
				elif not self.renderUnknown(name, value):
					raise Exception(f"bad value type {type(value)} in yaml producer")

				if self.addExtraSpacing:
					dictFormatter.addSpacing()

		def renderUnknown(self, name, value):
			return False

	class RpmList(AbstractListNode):
		def __init__(self, rpms = [], format = None):
			self.rpms = set(filter(lambda r: not r.isSynthetic, rpms))
			self.format = format

		def add(self, rpm):
			self.rpms.add(rpm)

		def render(self, listFormatter):
			if self.format == 'plain':
				for rpm in sorted(self.rpms, key = str):
					listFormatter.add(f"{rpm.name}")
				return

			for rpm in sorted(self.rpms, key = str):
				if rpm.labelHints is None or rpm.labelHints.epic is None:
					listFormatter.add(f"{rpm.name}")
				else:
					listFormatter.add(f"{rpm.name:30} # epic={rpm.labelHints.epic}")

	class AbstractComment(NodeBase):
		pass

	class CommentNode(AbstractComment):
		def __init__(self, comment):
			self.string = comment

		def __str__(self):
			return self.string


	def __init__(self):
		pass

	def createDictFormatter(self, ioStream):
		return YamlFormatter.Dict("", lambda x: print(x, file = ioStream))

	def createListFormatter(self, ioStream, indent = " "):
		return YamlFormatter.List(indent, lambda x: print(x, file = ioStream))

	def write(self, outputPath, reference = None, **kwargs):
		infomsg(f"Writing {outputPath}")
		with open(outputPath, "w") as ioStream:
			self.renderHeader(ioStream)

			print(f"# Automatically generated - do not edit", file = ioStream)
			if reference:
				print(f"# Generated from {reference}", file = ioStream)
			self.render(ioStream, **kwargs)

			self.renderTrailer(ioStream)

	def renderHeader(self, ioStream):
		pass

	def renderTrailer(self, ioStream):
		pass

class YamlDictProducerBase(YamlProducerBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def render(self, ioStream):
		dictFormatter = self.createDictFormatter(ioStream)
		self.root.render(dictFormatter)

class YamlMultiDictProducerBase(YamlProducerBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		self.documents = {}

	def createDocument(self, name):
		assert(name not in self.documents)

		doc = self.DictNode(addExtraSpacing = True)
		self.documents[name] = doc
		return doc

	def write(self, outputPath):
		assert('%id' in outputPath)

		for id, data in self.documents.items():
			name = outputPath.replace('%id', id)
			super().write(name, doc = data)

	def render(self, ioStream, doc = None):
		dictFormatter = self.createDictFormatter(ioStream)
		doc.render(dictFormatter)

