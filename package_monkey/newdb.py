import functools
import os

from .arch import *
from .filter import Classification
from .util import DictOfSets

__names__ = ['RpmInfo', 'GenericRpm', 'GenericBuild', 'NewDB', 'UniquePackageInfoFactory', 'ExtraDB']

class RpmInfo(object):
	def __init__(self, name, epoch, version, release, arch, buildArch = None):
		self.name = name
		self.epoch = epoch
		self.version = version
		self.release = release
		self.arch = arch
		self.buildArch = buildArch

		self.isSourcePackage = arch in ('src', 'nosrc')

	def __str__(self):
		return f"{self.name}.{self.arch}"

	@classmethod
	def parsePackageName(klass, pkgName, **kwargs):
		assert(pkgName.endswith('.rpm'))

		try:
			(n, arch, suffix) = pkgName.rsplit(".", maxsplit = 2)
			(name, version, release) = n.rsplit("-", maxsplit = 2)
		except:
			raise ValueError(f"Unable to parse RPM package name {pkgName}")

		return RpmInfo(name, None, version, release, arch, **kwargs)

class UniquePackageInfoFactory(object):
	def __init__(self, buildArch):
		self.buildArch = buildArch
		self._map = dict()

	def __call__(self, name, version, release, arch):
		key = f"{name}-{version}-{release}.{arch}"
		pinfo = self._map.get(key)
		if pinfo is None:
			pinfo = RpmInfo(name = name, version = version, release = release, arch = arch, epoch = None, buildArch = self.buildArch)
			self._map[key] = pinfo
		return pinfo

class NewDB(object):
	def __init__(self, traceMatcher = None):
		self.traceMatcher = traceMatcher

		self._rpms = {}
		self._sources = {}
		self._builds = {}
		self._promises = {}

		self.architectures = ArchSet()

		self.userVersion = None
		self.downloadTimestamp = None

		self._buildProvidesCache = False

	def addArchitecture(self, arch):
		self.architectures.add(arch)

	def lookupRpm(self, name):
		return self._rpms.get(name)

	def createRpm(self, name, type = None):
		rpm = self._rpms.get(name)
		if rpm is None:
			rpm = GenericRpm(name, type)
			if self.traceMatcher is not None and self.traceMatcher.match(name):
				rpm.trace = True
			self._rpms[name] = rpm
		elif type and rpm.type != type:
			raise Exception(f"{rpm}: cannot change type from {rpm.type} to {type}")

		return rpm

	def createSourceRpm(self, name):
		rpm = self._sources.get(name)
		if rpm is None:
			rpm = GenericSourceRpm(name)
			self._sources[name] = rpm
		return rpm

	def createRpmFromInfo(self, rpmInfo):
		if not rpmInfo.isSourcePackage:
			rpm = self.createRpm(rpmInfo.name)
		else:
			rpm = self.createSourceRpm(rpmInfo.name)

		# ignore buildArch and all of that
		return rpm

	@property
	def rpms(self):
		return iter(self._rpms.values())

	def lookupBuild(self, name):
		return self._builds.get(name)

	def createBuild(self, name):
		build = self._builds.get(name)
		if build is None:
			build = GenericBuild(name)
			if self.traceMatcher is not None and self.traceMatcher.match(name):
				build.trace = True
			self._builds[name] = build
		return build

	@property
	def builds(self):
		return iter(self._builds.values())

	def createPromise(self, rpm, arch = None):
		if arch is not None:
			name = f"promise:{arch}:{rpm}"
		else:
			name = f"promise:{rpm}"

		promise = self.createRpm(name, RpmBase.TYPE_PROMISE)
		self._promises[promise] = rpm
		return promise

	def lookupPromise(self, rpm):
		promise = self.lookupRpm(f"promise:{rpm}")
		if promise is not None:
			assert(promise.type == RpmBase.TYPE_PROMISE)
		return promise

	@property
	def promises(self):
		return iter(self._promises.keys())

	def promisedItems(self):
		return self._promises.items()

	def enableProvidesLookups(self):
		if self._buildProvidesCache:
			return
		self._buildProvidesCache = True

		for rpm in self.rpms:
			if rpm.isSourcePackage:
				continue
			rpm.requiredBy = rpm.DictOfSetsWithCommonTracking()
			for arch in rpm.architectures:
				rpm.requiredBy.update(arch, set())
				pass

		for rpm in self.rpms:
			commonRequires = rpm.resolvedRequires
			for req in commonRequires:
				for arch in rpm.architectures:
					req.requiredBy.add(arch, rpm)

			for arch in rpm.architectures:
				for req in rpm.solutions.raw_get(arch).difference(commonRequires):
					req.requiredBy.add(arch, rpm)

	def saveRpm(self, genericRpm, write):
		def writeDictOfSets(pfx, dos, archSet):
			common = dos.common
			if common:
				write(f"  {pfx} common {' '.join(sorted(map(str, common)))}")

			for arch in sorted(archSet):
				values = dos.get(arch)
				if not values:
					continue
				delta = values.difference(common)
				if delta:
					write(f"  {pfx} {arch} {' '.join(sorted(map(str, delta)))}")

		write(f"pkg {genericRpm.name} {genericRpm.type} {genericRpm.architectures}")
		writeDictOfSets('mem', genericRpm.controllingScenarios, genericRpm.architectures)
		writeDictOfSets('req', genericRpm.solutions, genericRpm.architectures)
		writeDictOfSets('scn', genericRpm.validScenarios, genericRpm.architectures)
		writeDictOfSets('ver', genericRpm.versions, genericRpm.architectures)
		writeDictOfSets('cond', genericRpm.conditionals, genericRpm.architectures)

		unrDict = genericRpm.unresolvables
		common = unrDict.common
		for dep in sorted(map(str, common)):
			write(f"  unr common {dep}")

		for arch in sorted(genericRpm.architectures):
			values = unrDict.get(arch)
			if not values:
				continue
			for dep in sorted(map(str, values.difference(common))):
				write(f"  unr {arch} {dep}")

	def save(self, path):
		def write(msg):
			print(msg, file = dbf)

		with open(path + ".tmp", "w") as dbf:
			if self.downloadTimestamp is not None:
				write(f"timestamp {self.downloadTimestamp}")

			write(f"arch {' '.join(sorted(self.architectures))}")

			syntheticTypes = set(RpmBase.VALID_TYPES)
			syntheticTypes.discard(RpmBase.TYPE_REGULAR)
			# We do not output any dependencies on rpms of type missing any more; so no
			# need to include them in the output file.
			syntheticTypes.discard(RpmBase.TYPE_MISSING)
			for type in sorted(syntheticTypes):
				rpms = filter(lambda r: r.type is type, self.rpms)
				for genericRpm in sorted(rpms, key = str):
					write(f"pkg {genericRpm.name} {genericRpm.type}")

			rpms = filter(lambda r: r.type is RpmBase.TYPE_REGULAR, self.rpms)
			for genericRpm in sorted(rpms, key = str):
				self.saveRpm(genericRpm, write)

			for build in self.builds:
				write(f"build {build.name}")
				for arch, status in build._buildStatus.items():
					if status != 'succeeded':
						write(f" status {arch} {status}")
				if build.controllingScenarioVersion:
					write(f" mem {build.controllingScenarioVersion}")
				for rpm in sorted(map(str, build.rpms)):
					write(f" rpm {rpm}")

		os.rename(path + ".tmp", path)
		infomsg(f"Updated {path}")

	def savePatch(self, path, rpms):
		def write(msg):
			print(msg, file = dbf)

		with open(path + ".tmp", "w") as dbf:
			ghostBuilds = set()
			for genericRpm in sorted(rpms, key = str):
				self.saveRpm(genericRpm, write)

				if genericRpm.isExternal:
					ghostBuilds.add(genericRpm.new_build)

			for build in ghostBuilds:
				write(f"build {build.name}")
				for rpm in sorted(map(str, build.rpms)):
					write(f" rpm {rpm} ghost")

		os.rename(path + ".tmp", path)
		infomsg(f"Updated {path}")

	def loadWorker(self, path, patching = False):
		nerrors = 0
		nrpms = 0
		nbuilds = 0

		def updateDictOfSets(dos, w, transform = None):
			key = w.pop(0)
			if transform is not None:
				w = set(map(transform, w))
			else:
				w = set(w)

			if key != 'common':
				dos.update(key, w)
			else:
				for arch in currentRpm.architectures:
					dos.update(arch, w)
				if dos._common is not None:
					dos._common.update(w)

		with open(path, 'r') as dbf:
			currentRpm = None
			currentBuild = None

			for line in dbf.readlines():
				w = line.split()
				cmd = w.pop(0)

				if cmd == 'timestamp':
					self.downloadTimestamp = ' '.join(w)
				elif cmd == 'arch':
					self.architectures.update(ArchSet(w))
				elif cmd == 'pkg':
					name = w.pop(0)
					type = None
					if w:
						type = w.pop(0)

					# Workaround, until we've cleaned up the prepare stage:
					# ignore promise:foo:arch style promises.
					if name.startswith("promise:") and name.count(":") > 1:
						continue

					rpm = self.createRpm(name, type)
					if w:
						rpm.architectures = ArchSet(w)

					if patching:
						# we're loading patch.db, so discard any existing data
						# on dependencies, version numbers etc.
						rpm.prepareToPatch()

					for arch in rpm.architectures:
						rpm.addDependencies(None, arch, set())

					currentRpm = rpm
					currentBuild = None
					nrpms += 1
				elif cmd == 'req':
					# Workaround, until we've cleaned up the prepare stage:
					# ignore promise:foo:arch style promises.
					saneNames = []
					for name in w:
						if name.startswith("promise:") and name.count(":") > 1:
							continue
						saneNames.append(name)
					w = saneNames

					assert(currentRpm)
					updateDictOfSets(currentRpm.solutions, w, transform = self.createRpm)
				elif cmd == 'mem':
					if currentRpm is not None:
						updateDictOfSets(currentRpm.controllingScenarios, w)
					elif currentBuild is not None:
						assert(len(w) == 1)
						currentBuild.controllingScenarioVersion = w[0]
				elif cmd == 'scn':
					assert(currentRpm)
					updateDictOfSets(currentRpm.validScenarios, w)
				elif cmd == 'ver':
					assert(currentRpm)
					updateDictOfSets(currentRpm.versions, w)
				elif cmd == 'unr':
					key = w.pop(0)
					dep = ' '.join(w)
					updateDictOfSets(currentRpm.unresolvables, [key, dep])
				elif cmd == 'cond':
					updateDictOfSets(currentRpm.conditionals, w)
				elif cmd == 'build':
					name = w.pop(0)

					# Workaround, until we've cleaned up the prepare stage:
					# ignore promise:foo:arch style promises.
					if name.startswith("promise:") and name.count(":") > 1:
						continue

					currentBuild = self.createBuild(name)
					currentRpm = None
					nbuilds += 1
				elif cmd == 'status':
					assert(currentBuild)

					arch, status = w
					currentBuild.setArchBuildStatus(arch, status)
				elif cmd == 'rpm':
					assert(currentBuild)
					name = w.pop(0)

					rpm = self.lookupRpm(name)
					if rpm is None:
						errormsg(f"DB {path}: build {currentBuild} references unknown rpm {name}")
						nerrors += 1
						continue

					for flag in w:
						if flag == 'ghost':
							rpm.isExternal = True
					currentBuild.addRpm(rpm)
				else:
					errormsg(f"DB {path}: command {cmd} not supported")
					nerrors += 1

		if nerrors:
			raise Exception(f"DB {path}: encountered {nerrors} errors")

		for rpm in self.rpms:
			if rpm.new_build is None and not rpm.isSynthetic:
				raise Exception(f"After loading DB: {rpm} w/o associated build")

		return nrpms, nbuilds

	def load(self, path):
		nrpms, nbuilds = self.loadWorker(path, patching = False)

		infomsg(f"DB {path}: loaded {nbuilds} builds and {nrpms} rpms")
		self.userVersion = int(os.stat(path).st_mtime)

	def loadPatch(self, path):
		nrpms, nbuilds = self.loadWorker(path, patching = True)
		infomsg(f"DB {path}: loaded {nbuilds} builds and {nrpms} rpms")

class RpmBase(object):
	TYPE_REGULAR	= 'rpm'
	TYPE_SYNTHETIC	= 'synthetic'
	TYPE_MISSING	= 'missing'
	TYPE_SCENARIO	= 'scenario'
	TYPE_PROMISE	= 'promise'
	TYPE_METAPKG	= 'meta'

	VALID_TYPES	= (TYPE_REGULAR, TYPE_SYNTHETIC, TYPE_MISSING, TYPE_SCENARIO, TYPE_PROMISE, TYPE_METAPKG)

	def __init__(self, name, type = None):
		self.name = name

		self._type = type or self.TYPE_REGULAR
		self.isSynthetic = (self._type != self.TYPE_REGULAR)
		self.isMissing = (self._type == self.TYPE_MISSING)
		self.isExternal = False

	@property
	def type(self):
		return self._type

	def __str__(self):
		if self.isSourcePackage:
			return f"{self.name}.src"
		return self.name


class GenericSourceRpm(RpmBase):
	isSourcePackage = True

	def __init__(self, name, type = None):
		super().__init__(name, type)

		self.new_build = None
		self.new_override_epic = None

class GenericRpm(RpmBase):
	isSourcePackage = False

	class DictOfSetsWithCommonTracking(DictOfSets):
		def __init__(self):
			super().__init__()
			self._common = None

		def __bool__(self):
			return bool(self.common) or super().__bool__()

		def raw_get(self, key):
			return super().get(key)

		@property
		def common(self):
			if self._common is None:
				if self._dict:
					self._common = functools.reduce(set.intersection, self.values())
				else:
					self._common = set()
			return self._common

		def allIdentical(self):
			common = self.common
			return all((s == common) for s in self.values())

		def discard(self, key, value):
			if self._common is not None and value in self._common:
				self._common = None

			super().discard(key, value)

		def clear(self):
			super().clear()
			self._common = None

	def __init__(self, name, type = None):
		super().__init__(name, type)

		self.architectures = ArchSet()
		self.missingArchitectures = ArchSet()
		# FIXME: rename solutions -> requires
		self.solutions = self.DictOfSetsWithCommonTracking()
		self.requiredBy = None
		self.validScenarios = self.DictOfSetsWithCommonTracking()
		self.controllingScenarios = self.DictOfSetsWithCommonTracking()
		self.unresolvables = self.DictOfSetsWithCommonTracking()
		self.versions = self.DictOfSetsWithCommonTracking()
		self.conditionals = self.DictOfSetsWithCommonTracking()

		# used by the 3rd stage only
		self.labelHints = None
		self.trace = False

		# backward compat
		self.fullname = self.name

		self.new_build = None
		self.new_class = None
		self.new_override_epic = None

		self.isUnresolvable = False
		if name == '__unresolved__':
			self.isUnresolvable = True

	def __str__(self):
		return self.name

	def addDependencies(self, dep, arch, rpmNames, unresolvable = False):
		self.architectures.add(arch)
		self.solutions.update(arch, rpmNames)

		if unresolvable:
			self.unresolvables.add(arch, str(dep))
		else:
			# make sure we have an empty set of unresolvable dependencies
			# for this arch; otherwise, unresolvables.common will be computed
			# only over those architectures that actually do have any
			# unresolvables.
			self.unresolvables.update(arch, set())

	def getDependencies(self, arch):
		return self.solutions.get(arch)

	def addScenarios(self, arch, scenarioChoices):
		self.validScenarios.update(arch, scenarioChoices)

	def getScenarios(self, arch):
		return self.validScenarios.get(arch)

	def addControllingScenarios(self, arch, scenarioChoices):
		self.controllingScenarios.update(arch, scenarioChoices)

	def getControllingScenarios(self, arch):
		return self.controllingScenarios.get(arch)

	def addConditional(self, arch, cond):
		self.conditionals.add(arch, str(cond.parsed))

	def getConditionals(self, arch):
		return self.conditionals.get(arch)

	@property
	def resolvedRequires(self):
		return self.solutions.common

	def addVersion(self, arch, version):
		self.versions.add(arch, version)

	@property
	def isIgnored(self):
		if self.labelHints is None:
			return False
		return self.labelHints.isIgnored

	def supportsExpectedArchitectures(self, archSet):
		if self.type != self.TYPE_REGULAR:
			return True
		return archSet.issubset(self.architectures)

	def setLabelHints(self, labelHints):
		if labelHints is None:
			return

		# Do not label promises and the like with roles or class labels:
		if self.type != self.TYPE_REGULAR:
			if labelHints.epic is None and labelHints.definingBuildOption is None:
				return

		if self.trace:
			infomsg(f"{self}: setting label hints to {labelHints}. my arch set={self.architectures}")
			if labelHints.epic and labelHints.epic.architectures:
				infomsg(f"   arch set={labelHints.epic.architectures}")

			if labelHints.overrideArch is not None:
				infomsg(f"   override arch {labelHints.overrideArch}")
			else:
				if labelHints.includeArch is not None:
					infomsg(f"   add arch {labelHints.includeArch}")
				if labelHints.excludeArch is not None:
					infomsg(f"   drop arch {labelHints.excludeArch}")

			if labelHints is not None:
				infomsg(f"   label hints {labelHints}")

		self.labelHints = labelHints
		labelHints.inuse = True

		if labelHints is not None:
			if self.new_class is not None and labelHints.klass is not self.new_class:
				errormsg(f"XXX: {self}: my klass={self.new_class}; cannot overwrite with {labelHints.klass}")
			if self.new_class is None and labelHints.klass is not None:
				self.new_class = labelHints.klass

			epic = labelHints.epic
			build = self.new_build
			if build is not None and epic is not None:
				if build.new_epic is None:
					# This is still happening for promise:* rpms, as we often place the
					# promises via class contexts:
					# warnmsg(f"{self}: need to overwrite build {build} epic {build.new_epic} with {epic}")
					self.new_build.new_epic = epic
					self.new_build.layer = epic.layer
				if build.new_epic is not epic:
					self.new_override_epic = epic

			if labelHints.overrideArch is not None:
				self.architectures = labelHints.overrideArch
			else:
				if labelHints.includeArch is not None:
					self.architectures.update(labelHints.includeArch)
				if labelHints.excludeArch is not None:
					self.architectures.difference_update(labelHints.excludeArch)

	def unshareLabelHints(self):
		if self.labelHints is not None:
			self.labelHints = self.labelHints.unshare()
		return self.labelHints

	# hack
	@property
	def label(self):
		raise Exception(f"rpm.label no longer supported")

	# backwards compatibility
	def enumerateRequiredRpms(self):
		for rpm in self.solutions.common:
			yield rpm

	def enumerateUnresolvedDependencies(self):
		for dep in self.unresolvables.common:
			yield dep

	@property
	def validForScenarios(self):
		return self.validScenarios.common.copy()

	def getValidScenarios(self, archSet = None):
		if archSet is None:
			return self.validScenarios.common.copy()

		result = None
		for arch in archSet:
			archScenarios = self.validScenarios.get(arch)
			if archScenarios is None:
				result = set()
			elif result is None:
				result = archScenarios
			else:
				result = result.intersection(archScenarios)
		return result

	def replaceDependency(self, oldReq, newReq, arch = None):
		dos = self.solutions
		if arch is not None:
			dos.discard(arch, oldReq)
			dos.add(arch, newReq)
			dos._common = None
		else:
			for arch in self.architectures:
				dos.discard(arch, oldReq)
				dos.add(arch, newReq)
			if dos._common is not None:
				dos._common.add(newReq)

	# This is invoked when patching up the codebase with fake rpms from the ghosts section.
	def prepareToPatch(self):
		for dos in (self.solutions, self.versions, self.validScenarios, self.controllingScenarios, self.unresolvables,):
			dos.clear()

class GenericBuild(object):
	def __init__(self, name):
		self.name = name
		self.rpms = set()
		self.source = None

		self._buildStatus = {}

		# used by the 3rd stage only
		self.isSynthetic = False
		self.labelHints = None
		self.trace = False

		self.new_epic = None
		self.new_layer = None

		self.controllingScenarioVersion = None

	def __str__(self):
		return self.name

	def setArchBuildStatus(self, arch, status):
		self._buildStatus[arch] = status

	def getArchBuildStatus(self, arch):
		return self._buildStatus.get(arch, 'excluded')

	@property
	def binaries(self):
		return self.rpms

	@property
	def sourceRpm(self):
		return self.source

	@property
	def layer(self):
		return self.new_layer

	@layer.setter
	def layer(self, layer):
		if self.new_layer is not None and self.new_layer is not layer:
			raise Exception(f"build {self}: conflicting layer information {self.new_layer} vs {layer}")
		self.new_layer = layer

	@property
	def epic(self):
		return self.new_epic

	@epic.setter
	def epic(self, epic):
		if self.new_epic is not None and self.new_epic is not epic:
			raise Exception(f"build {self}: conflicting epic information {self.new_epic} vs {epic}")
		self.new_epic = epic
		self.layer = epic.layer

	def addRpm(self, rpm):
		if rpm.new_build is None:
			rpm.new_build = self
		elif rpm.new_build is not self:
			# We do not print this message here anymore; the ClassificationGadget can generate a
			# report about this; and the classification command displays it.
			# errormsg(f"Conflicting builds for {rpm.isSourcePackage and 'source' or 'binary'} rpm {rpm}: {self} vs {rpm.new_build}")
			pass

		if rpm.isSourcePackage:
			if self.source is not None and self.source is not rpm:
				raise Exception(f"{self}: conflicting source packages {self.source} vs {rpm}")
			self.source = rpm
		else:
			self.rpms.add(rpm)

	def setLabelHints(self, labelHints):
		if labelHints is None or labelHints.epic is None:
			raise Exception(f"Cannot assign build {self} to epic: label hints={labelHints}")

		epic = labelHints.epic
		self.new_epic = epic
		self.layer = epic.layer
		self.labelHints = labelHints

		if labelHints.overrideArch or \
		   labelHints.includeArch or \
		   labelHints.excludeArch:
			warnmsg(f"{self} ignoring architecture overrides from label hints")

		# I'm still displaying these messages because the syntax may be a bit misleading/surprising for these things.
		if labelHints.klass:
			infomsg(f"EXPERIMENTAL: using class={labelHints.klass} hints in build pattern for {self}")
		if labelHints.options:
			infomsg(f"EXPERIMENTAL: using option={labelHints.options} hints in build pattern for {self}")

	@property
	def uniformArchitectures(self):
		archSet = archRegistry.fullset
		inspected = []
		for rpm in self.binaries:
			# We should probably do this much earlier
			if rpm.name.endswith('-debugsource') or \
			   rpm.name.endswith('-debuginfo'):
				continue

			if rpm.type != rpm.TYPE_REGULAR:
				continue

			archSet = archSet.intersection(rpm.architectures)
			inspected.append(rpm)

		if archSet != archRegistry.fullset:
			if self.trace:
				infomsg(f"{self} not available on all architectures: {archSet}")
				for rpm in inspected:
					infomsg(f"  {rpm} {rpm.architectures}")

			good = all((rpm.architectures == archSet) for rpm in inspected)
			if not good:
				return None

		return archSet

	@property
	def buildIssues(self):
		for arch, status in self._buildStatus.items():
			if status != 'succeeded':
				yield arch, status

	@property
	def successful(self):
		return not any(self.buildFailures)

	@property
	def buildFailures(self):
		for arch, status in self._buildStatus.items():
			if status not in ('succeeded', 'excluded', 'blocked', 'scheduled', 'building'):
				yield arch, status

	@property
	def commonBuildVersion(self):
		versions = set()
		for rpm in self.binaries:
			rpmVersions = rpm.versions.common
			if len(rpmVersions) != 1:
				return None
			versions.update(rpmVersions)
		if len(versions) != 1:
			return None
		return next(iter(versions))

class GenericScenarioClass(object):
	def __init__(self, name, values, partiallyPresent = None):
		self.name = name
		self.values = set(values)
		self.partiallyPresent = set()

		if partiallyPresent:
			self.values.update(partiallyPresent)
			self.partiallyPresent.update(partiallyPresent)


	def markPartiallySupported(self, values):
		self.partiallyPresent.update(self.values.intersection(values))

##################################################################
# We store additional rpm information such as summary and
# descriptions in a separate DB.
#
# FIXME: the data representation could be way more compact.
##################################################################
class ExtraDB(object):
	def __init__(self):
		self._rpms = {}
		self._foundNames = set()

	@staticmethod
	def makekey(name, buildArch):
		return f"{name}.{buildArch}"

	def lookupRpm(self, name, buildArch, create = False):
		key = self.makekey(name, buildArch)
		rpm = self._rpms.get(key)
		if rpm is None:
			rpm = RpmAuxInfo(name, buildArch)
			self._rpms[key] = rpm

		return rpm

	def maybeUpdate(self, rpmName, buildArch, hash):
		self._foundNames.add(self.makekey(rpmName, buildArch))

		rpmInfo = self.lookupRpm(rpmName, buildArch, create = True)
		if rpmInfo.hash == hash:
			# no need to update
			return None

		return rpmInfo

	def removeStaleEntries(self):
		removed = set(self._rpms.keys()).difference(self._foundNames)

		if removed:
			infomsg(f"Removing {len(removed)} stale entries")

		for key in removed:
			infomsg(f"delete {key}")
			del self._rpms[key]

	def save(self, path):
		with open(path, "w") as dbf:
			def write(msg):
				print(msg, file = dbf)

			for key, rpmInfo in sorted(self._rpms.items()):
				write(f"rpm {rpmInfo.name} {rpmInfo.arch} {rpmInfo.hash}")
				for attr in 'version', 'release', 'summary', 'buildtime', 'description':
					value = getattr(rpmInfo, attr, None)
					if not value:
						continue

					if type(value) is str and '\n' in value:
						lines = value.strip().split('\n')
						if not lines:
							continue
						write(f"   {attr} |")
						for l in lines:
							write(f"   |{l}")
					else:
						write(f"   {attr} {value}")

	def load(self, path):
		continuation = None
		continuationAttr = None
		currentRpm = None

		with open(path) as dbf:
			for line in dbf.readlines():
				line = line.strip()

				if continuation is not None:
					if line.startswith('|'):
						continuation.append(line[1:].lstrip())
						continue
					setattr(currentRpm, continuationAttr, '\n'.join(continuation))
					continuation = None

				cmd, data = line.split(maxsplit = 1)
				if cmd == 'rpm':
					name, arch, hash = data.split()
					currentRpm = self.lookupRpm(name, arch, create = True)
					currentRpm.hash = hash
				elif cmd in ('buildtime',):
					setattr(currentRpm, cmd, int(data))
				elif cmd in ('version', 'release', 'summary', 'buildtime', 'description'):
					if data != '|':
						setattr(currentRpm, cmd, data)
					else:
						continuationAttr = cmd
						continuation = []
				else:
					raise Exception(f"{path}: unknown keyword {cmd}")

class RpmAuxInfo(object):
	def __init__(self, name, arch, hash = None):
		self.name = name
		self.arch = arch
		self.hash = hash

		self.version = None
		self.release = None
		self.summary = None
		self.description = None
		self.buildTime = 0

	def __str__(self):
		return f"{self.name}.{self.arch}"

	def check(self, hash):
		return self.hash == hash

	def update(self, d, hash):
		assert(self.name == d['name'])

		for attr, value in d.items():
			if attr in ('name', 'arch'):
				continue

			setattr(self, attr, value)

		self.hash = hash

