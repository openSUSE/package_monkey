##################################################################
#
# package classes
#
##################################################################

from .util import debugmsg, infomsg, warnmsg, errormsg
from .arch import *

# FIXME: should this move to newdb.py?
class PackageCollection(object):
	def __init__(self):
		self._packages = set()
		self._builds = []
		self._sources = set()
		self._packageDict = {}
		self._archDict = {}

	def copy(self):
		result = self.__class__()

		result._packages = self._packages.copy()
		result._builds = self._builds.copy()
		result._sources = self._sources.copy()
		result._packageDict = self._packageDict.copy()
		result._archDict = self._archDict.copy()

		return result

	def __len__(self):
		return len(self._packageDict)

	def __iter__(self):
		return iter(self.packages)

	def rpmsWithArch(self):
		for rpm in self:
			yield rpm, self._archDict.get(rpm.name)

	def get(self, name):
		return self._packageDict.get(name)

	def getArch(self, arg):
		if type(arg) is not str:
			arg = str(arg)
		return self._archDict.get(arg)

	# Add an RPM to the collection.
	# If the package is already present, update its arch set
	def add(self, rpm, archSet = None, overwriteArch = False):
		self._packages.add(rpm)
		self._packageDict[rpm.name] = rpm

		if archSet is not None:
			if not archSet.issubset(rpm.architectures):
				raise Exception(f"Invalid arch set {archSet} for {rpm}: rpm does not support {archSet.difference(rpm.architectures)}")

			if overwriteArch or rpm.name not in self._archDict:
				self._archDict[rpm.name] = archSet.copy()
			else:
				self._archDict[rpm.name].update(archSet)

	def discard(self, rpm, archSet = None):
		if rpm not in self._packages:
			return

		resultingArchSet = None
		if archSet is not None:
			existingArchSet = self._archDict.get(rpm.name)
			if existingArchSet is None:
				existingArchSet = rpm.architectures
			resultingArchSet = existingArchSet.difference(archSet)

		if resultingArchSet:
			self._archDict[rpm.name] = resultingArchSet
		else:
			self._packages.discard(rpm)
			del self._packageDict[rpm.name]
			try:
				del self._archDict[rpm.name]
			except: pass

	def addBuild(self, build):
		self._builds.append(build)
		for rpm in build.binaries:
			if rpm.name.endswith('-debuginfo') or rpm.name.endswith('-debugsource'):
				continue
			if not rpm.isSourcePackage:
				self.add(rpm)
			else:
				self._sources.add(rpm)

	@property
	def builds(self):
		return iter(self._builds)

	@property
	def packages(self):
		return iter(self._packages.union(self._sources))

	# set operations
	def update(self, other):
		assert(isinstance(other, self.__class__))

		for rpm, archSet in other.rpmsWithArch():
			if archSet is None:
				archSet = rpm.architectures
			self.add(rpm, archSet)

	def difference_update(self, other):
		assert(isinstance(other, self.__class__))
		for rpm, archSet in other.rpmsWithArch():
			self.discard(rpm, archSet)

	def union(self, other):
		ret = self.copy()
		ret.update(other)
		return ret

	def difference(self, other):
		ret = self.copy()
		ret.difference_update(other)
		return ret

	def enablePackageTracing(self, traceMatcher):
		for rpm in self.packages:
			if rpm.isSourcePackage:
				continue
			if traceMatcher.match(rpm.name):
				infomsg(f"Tracing rpm {rpm}")
				rpm.trace = True

		for build in self.builds:
			if traceMatcher.match(build.name):
				infomsg(f"Tracing build {build}")
				build.trace = True

##################################################################
# This is used in various places during classification and
# composition to deal with RPMs that should be there but aren't,
# or as a quick-n-dirty override of composer decisions.
##################################################################
class RpmOverrideList(object):
	class Entry(object):
		def __init__(self, name, archSet = None, version = None):
			self.name = name
			self.archSet = archSet
			self.version = version or "0.0"

		def __str__(self):
			if self.archSet is not None:
				return f"{self.name}: [{self.archSet}]"
			return self.name

	def __init__(self):
		self.items = {}

	def __bool__(self):
		return bool(self.items)

	def __len__(self):
		return len(self.items)

	def __iter__(self):
		return iter(sorted(self.items.values(), key = lambda i: i.name))

	def __contains__(self, item):
		if type(item) is str:
			return item in self.items
		return item.name in self.items

	def add(self, item):
		assert(isinstance(item, self.Entry))
		self.items[item.name] = item

	def discard(self, name):
		try:
			del self.items[name]
			return True
		except:
			pass
		return False

	# Note, entries from "other" do not overwrite entries in self that have the same key
	def update(self, other):
		assert(isinstance(other, self.__class__))

		for item in other:
			if item not in self:
				self.add(item)

	def difference_update(self, other):
		assert(isinstance(other, self.__class__))

		for item in other:
			self.discard(item.name)

	def union(self, other):
		assert(isinstance(other, self.__class__))

		result = self.__class__()
		result.items = self.items.copy()
		result.update(other)
		return result

	def difference(self, other):
		assert(isinstance(other, self.__class__))

		result = self.__class__()
		result.items = self.items.copy()
		result.difference_update(other)
		return result

	def toRpms(self, db, create = False, quiet = True):
		result = PackageCollection()
		nerrors = 0

		for item in self:
			rpm = db.lookupRpm(item.name)
			if rpm is not None:
				newArchSet = item.archSet or rpm.architectures
				if item.archSet is not None:
					if rpm.trace:
						infomsg(f"Override {rpm}: {item.archSet}")
					if not item.archSet.issubset(rpm.architectures):
						missing = item.archSet.difference(rpm.architectures)
						newArchSet = newArchSet.difference(missing)
						warnmsg(f"Override {rpm}: these architectures are disabled: {missing}, skipping")

				result.add(rpm, newArchSet)
				continue

			if create:
				rpm = db.createRpm(item.name)
				rpm.architectures.update(item.archSet or archRegistry.fullset)
				rpm.isExternal = True
				for arch in rpm.architectures:
					rpm.addVersion(arch, item.version)

				build = db.createBuild(f"{item.name}:build")
				build.addRpm(rpm)

				if not quiet:
					infomsg(f"Creating ghost rpm {rpm} [{rpm.architectures}]")
				result.add(rpm, rpm.architectures)
				continue

			if not item.archSet.issubset(db.architectures):
				warnmsg(f"Override {item.name}: skipping as not available for the enabled architectures")
				continue

			errormsg(f"override list specifies unknown rpm {item.name}")
			nerrors += 1

		if nerrors:
			raise Exception(f"unknown rpm names in override list")

		return result

##################################################################
# The following classes help us detect soversion changes (ie
# when rpm libfoobar1 is replaced with libfoobar2).
##################################################################
class LibraryPackageMap(object):
	def __init__(self, nameSet):
		self.mapping = {}

		ambiguous = set()
		for name in nameSet:
			if not name.startswith('lib'):
				continue

			stem = name
			stemplus = ""

			for suffix in ("-32bit", "-x86-64-v3"):
				if stem.endswith(suffix):
					stemplus = f"{suffix}{stemplus}"
					stem = stem[:-len(suffix)]

			while stem[-1].isdigit():
				stem = stem.rstrip("0123456789")
				for suffix in ('_alpha', '_beta', '_rc'):
					if stem.endswith(suffix):
						stem = stem[:-len(suffix)]
						break

				if stem[-1] in ('_', '-'):
					stem = stem[:-1]

			# glue suffixes back on
			stem += stemplus

			if stem in self.mapping:
				ambiguous.add(stem)

			self.mapping[stem] = name

		self.stems = set(self.mapping.keys()).difference(ambiguous)
		self.names = set(self.mapping[stem] for stem in self.stems)

	def get(self, stem):
		return self.mapping[stem]

class RpmNameClassification(object):
	def __init__(self, oldNames, newNames):
		self.oldNames = set(oldNames)
		self.newNames = set(newNames)

		self.commonNames = self.oldNames.intersection(self.newNames)

		self.oldLibraries = LibraryPackageMap(self.oldNames.difference(self.commonNames))
		self.newLibraries = LibraryPackageMap(self.newNames.difference(self.commonNames))
		self.sharedLibraryNames = self.oldLibraries.stems.intersection(self.newLibraries.stems)

		self.removedNames = self.oldNames.difference(self.commonNames).difference(self.oldLibraries.names)
		self.addedNames = self.newNames.difference(self.commonNames).difference(self.newLibraries.names)

	# returns true if there are changes
	def __bool__(self):
		return bool(self.addedNames or self.removedNames or self.sharedLibraryNames)

	@property
	def soversionChanges(self):
		for name in self.sharedLibraryNames:
			yield self.oldLibraries.get(name), self.newLibraries.get(name)

