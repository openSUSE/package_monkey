#
# This is the second step. For a set of repositories, use libsolv to resolve all
# package dependencies.
# We allow a certain degree of ambiguity, but disambiguate dependencies that cover
# eg different versions of java.
# Output a package DB that contains the generic dependencies

import solv
import os
import re
import functools

from .util import debugmsg, infomsg, warnmsg, errormsg, loggingFacade
from .util import ThatsProgress, TableFormatter
from .options import ApplicationBase
from .preprocess import *
from .scenario import *
from .newdb import NewDB
from .reports import GenericStringReport
from .arch import *

class PreprocessApplicationBase(ApplicationBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		self.repositoryCollection = None
		self.architectures = set()
		self.hints = None
		self.pedantic = False
		self.traceDisambiguation = False
		self.ignoreErrors = False

		self.resolverLog = None
		self.errorReport = GenericStringReport()

	def openResolverLog(self):
		if self.opts.reslog is None:
			self.opts.reslog = self.getCodebasePath("resolver.log")
		self.resolverLog = ResolverLog(self.opts.reslog)

	def loadRepositories(self, withStaging = None):
		solverDir = self.getCachePath('solve')
		self.repositoryCollection = SolverRepositoryCollection.fromCodebase(self.productCodebase, solverDir)

		if withStaging is not None:
			self.repositoryCollection.enableStaging(withStaging)

		self.architectures = self.repositoryCollection.architectures

	def overrideArchitectures(self, archList):
		self.architectures = ArchSet()
		for s in self.opts.only_arch:
			self.architectures.update(ArchSet(s.split(',')))

	def createArchSolver(self, arch):
		archSolver = ArchSolver(arch, hints = self.hints, traceMatcher = self.traceMatcher, errorReport = self.errorReport)

		for repository in self.repositoryCollection:
			if repository.arch == arch:
				archSolver.addRepository(repository)

		archSolver.resolverLog = self.resolverLog
		archSolver.pedantic = self.pedantic
		archSolver.traceDisambiguation = self.opts.trace_scenarios

		return archSolver

	def updateRpm(self, db, arch, result):
		rpmName = result.requiringPkg.shortname
		unresolvedRpm = db.lookupRpm('__unresolved__')

		genericRpm = db.createRpm(rpmName)
		genericRpm.architectures.add(arch)

		for archSpecificDep in result:
			solution = archSpecificDep.solutions
			if not solution:
				solution = archSpecificDep.alternatives

			required = set()
			for rpm in solution:
				if rpm.isMissing:
					# convert "known missing" rpm back into unresolved.
					required.add(unresolvedRpm)
				else:
					required.add(db.createRpm(rpm.shortname))

			genericRpm.addDependencies(str(archSpecificDep.dep), arch, required,
						unresolvable = (unresolvedRpm in required))

		for cond in result.conditionals:
			genericRpm.addConditional(arch, cond)

		if result.validScenarioChoices is not None:
			genericRpm.addScenarios(arch, set(map(str, result.validScenarioChoices)))

		if result.controllingScenarios:
			genericRpm.addControllingScenarios(arch, set(map(str, result.controllingScenarios)))

		version = result.version
		if version is not None:
			genericRpm.addVersion(arch, version)

		if result.requiringPkg.isExternal:
			debugmsg(f"creating synthetic build for external rpm {result.requiringPkg}")
			build = db.createBuild(f"{rpmName}:build")
			build.addRpm(genericRpm);
			build.isSynthetic = True

		return genericRpm

	def displayUnresolvables(self, unresolvables):
		pass

	def updateCodebasePatch(self):
		db = self.loadNewDB(withoutPatchDB = True)

		unresolvables = self.getUnresolvables(db)
		self.displayUnresolvables(unresolvables)

		codebaseModel = self.modelDescription.codebaseModel
		if codebaseModel.ghostRpms is None:
			infomsg(f"Codebase does not define any ghosts. Not patching anything.")
			return

		ghosts = codebaseModel.ghostRpms.toRpms(db, create = True)

		self.hints = self.modelDescription.loadPreprocessorHints()
		self.loadRepositories(withStaging = self.opts.staging)

		for rpm in unresolvables:
			rpm.prepareToPatch()

		for arch in codebaseModel.architectures:
			archSolver = self.createArchSolver(arch)

			rpmsToSolve = []
			for rpm in unresolvables:
				if arch not in rpm.architectures:
					continue
				rpm = archSolver.nameToRpm(rpm.name)
				assert(rpm is not None)
				rpmsToSolve.append(rpm)

			if not rpmsToSolve:
				continue

			for rpm in ghosts:
				if arch not in rpm.architectures:
					continue

				versions = rpm.versions.get(arch)
				assert(len(versions) == 1)
				version = next(iter(versions))

				rpm = archSolver.createDummySolvable(rpm.name, evr = f"{version}-1", type = rpm.TYPE_REGULAR)
				rpm.isExternal = True
				infomsg(f"created external {rpm}-{rpm.solvable.evr}")

			archSolver.solve(progressMeter = None, rpms = rpmsToSolve)

			for result in archSolver.resolvedRpms:
				genericRpm = self.updateRpm(db, arch, result)

		db.savePatch(self.codebaseData.patchPath, unresolvables.union(ghosts))

	def getUnresolvables(self, db):
		unresolvables = set()
		for build in db.builds:
			for rpm in build.binaries:
				if rpm.isSynthetic:
					continue

				if rpm.unresolvables:
					unresolvables.add(rpm)

		return unresolvables

class SolverApplication(PreprocessApplicationBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def run(self):
		self.ignoreErrors = self.opts.ignore_errors
		self.traceDisambiguation = self.opts.trace_scenarios
		self.pedantic = self.opts.pedantic
		if self.opts.trace:
			self.traceDisambiguation = True

		self.hints = self.modelDescription.loadPreprocessorHints()
		self.loadRepositories(withStaging = self.opts.staging)

		if self.opts.only_arch:
			self.overrideArchitectures(self.opts.only_arch)

		infomsg(f"Using the following repositories:")
		for repository in self.repositoryCollection:
			infomsg(f"   {repository}")

		self.openResolverLog()

		archSolvers = []
		for arch in sorted(self.architectures):
			archSolvers.append(self.createArchSolver(arch))

		totalRpmCount = sum(len(a.queue) for a in archSolvers)
		progressMeter = ThatsProgress(totalRpmCount)

		db = NewDB(traceMatcher = self.traceMatcher)
		for repository in self.repositoryCollection:
			repository.loadBuilds(db)

		for archSolver in archSolvers:
			archSolver.solve(progressMeter, db = db)

		for archSolver in archSolvers:
			self.extractResolution(archSolver, db)

		self.displayUnresolved(db)
		self.collapseResults(db)

		info = self.codebaseData.loadDownloadInfo()
		db.downloadTimestamp = info.timestamp

		self.saveDB(db)

		if self.errorReport:
			self.errorReport.display()
			if not self.opts.ignore_errors:
				return 1

		self.updateCodebasePatch()

		return 0

	def extractResolution(self, archSolver, db):
		arch = archSolver.arch

		db.addArchitecture(arch)

		for rpm in archSolver.getAllRpms(RpmBase.TYPE_MISSING):
			genericRpm = db.createRpm(rpm.shortname)
			genericRpm.missingArchitectures.add(arch)

		for type in (RpmBase.TYPE_SYNTHETIC, RpmBase.TYPE_SCENARIO, RpmBase.TYPE_PROMISE):
			for rpm in archSolver.getAllRpms(type):
				assert(rpm.type == type)
				genericRpm = db.createRpm(rpm.shortname, type)

		for result in archSolver.resolvedRpms:
			self.updateRpm(db, arch, result)

	def displayBuildFailures(self, db):
		tableFormatter = TableFormatter(["name"] + list(map(str, self.architectures)),
					[50, 12, 12, 12, 12, 12, 12])
		for build in db.builds:
			if build.successful:
				continue

			row = tableFormatter.addRow(build.name)
			for arch, status in build.buildFailures:
				row[arch] = status

		tableFormatter.render("The following builds seem to be failing", displayfn = infomsg)

	def displayBuildsWithVersionDrift(self, db):
		tableFormatter = TableFormatter(["name"] + list(map(str, self.architectures)),
					[50, 12, 12, 12, 12, 12, 12])
		for build in db.builds:
			if not build.successful or build.isSynthetic:
				continue

			if self.hints and build.name in self.hints.buildNoVersionCheckSet:
				continue

			for rpm in build.rpms:
				if not rpm.isSynthetic and rpm.versions and not rpm.versions.common:
					row = tableFormatter.addRow(rpm.name)
					for arch, vset in rpm.versions.items():
						row[arch] = f"{' '.join(vset)}"

		tableFormatter.render("The following rpms have version drift", displayfn = infomsg)

	def displayUnresolved(self, db, missingCutoff = 7):
		tableFormatter = TableFormatter(["name"] + list(map(str, self.architectures)) + ["deps"],
					[50, 8, 8, 8, 8, 8, 8])
		for rpm in db.rpms:
			if rpm.isSynthetic or not rpm.unresolvables:
				continue

			row = tableFormatter.addRow(rpm.name)
			overallMissing = set()

			for arch in rpm.architectures:
				missingDeps = rpm.unresolvables.get(arch)
				if not missingDeps:
					row[arch] = f"-"
					continue

				overallMissing.update(missingDeps)
				if not self.hints.filterUnresolvedRequirements(rpm.name, missingDeps):
					row[arch] = f"(known)"
					continue

				row[arch] = f"YES"

			overallMissing = sorted(map(str, overallMissing))
			if len(overallMissing) > missingCutoff:
				overallMissing[missingCutoff - 1] = '...'
			row['deps'] = '; '.join(map(str, overallMissing[:missingCutoff]))

		tableFormatter.render("The following rpms have unresolved dependencies", displayfn = infomsg)

	def collapseResults(self, db):
		for genericRpm in db.rpms:
			# FIXME: we should mark architecture as missing *only* if it was required by something
			if not genericRpm.architectures and genericRpm.missingArchitectures == db.architectures:
				# fudge the rpm type:
				genericRpm._type = RpmBase.TYPE_MISSING
				genericRpm.isSynthetic = True
				continue

			if genericRpm.validScenarios and not genericRpm.validScenarios.common:
				if not self.scenarioDetective(db, genericRpm):
					warnmsg(f"{genericRpm}: problematic combination of scenarios")
					for arch in genericRpm.architectures:
						scenarios = list(genericRpm.getScenarios(arch) or [])
						infomsg(f"    - {arch}: {' '.join(map(str, scenarios))}")

			if genericRpm.solutions.allIdentical():
				unresolvableDeps = genericRpm.unresolvables.common

				if unresolvableDeps:
					unresolvableDeps = self.hints.filterUnresolvedRequirements(genericRpm.name, unresolvableDeps)

				if unresolvableDeps and genericRpm.trace:
					infomsg(f"{genericRpm} has unresolvable dependencies on all architectures: {unresolvableDeps}")
				continue

			common = genericRpm.solutions.common 
			for arch in genericRpm.architectures:
				solution = genericRpm.getDependencies(arch)
				delta = solution.difference(common)

				# If a package is unresolvable on _all_ architectures, the package will show up as
				# depending on __unresolved__.
				# If it is unresolved on just some architectures, we disable the package for this
				# architecture.
				badDependencies = genericRpm.unresolvables.get(arch)
				if badDependencies:
					if genericRpm.trace:
						infomsg(f"{genericRpm} is unresolvable on {arch} - disabling package on this architecture")
					genericRpm.architectures.remove(arch)

		self.displayBuildFailures(db)
		self.displayBuildsWithVersionDrift(db)

		# Try to detect when a build like rust1.99 provides rpms for just a single scenario version
		# Not all packages covered by scenarios do that; for example, build systemd-default-settings
		# spits out a bunch of rpms, for different products, and hence for different product=XXX scenarios.
		for build in db.builds:
			controllingScenarios = set()
			for rpm in build.binaries:
				rpmScenarios = functools.reduce(set.union, rpm.controllingScenarios.values(), set())
				controllingScenarios.update(rpmScenarios)

			if build.trace and controllingScenarios:
				infomsg(f"{build}: rpms covered by {' '.join(map(str, controllingScenarios))}")

			# up to this point, the scenario set contains just strings formatted as var/version/rpmname;
			# now parse it into a set of ScenarioTuples:
			controllingScenarios = ScenarioTupleSet(map(ScenarioTuple.parse, controllingScenarios))
			controllingVersions = controllingScenarios.versions

			if len(controllingVersions) == 1:
				build.controllingScenarioVersion = next(iter(controllingVersions))

		if self.displayAmbiguousBuilds(db) and not self.ignoreErrors:
			raise Exception(f"Encountered {nAmbiguityErrors} build ambiguity error(s)")
	
	# Check for RPMs that are generated by multiple builds
	def displayAmbiguousBuilds(self, db):
		rpmMap = {}
		for build in db.builds:
			for rpm in build.rpms:
				existing = rpmMap.get(rpm)
				if existing is None or existing is build:
					rpmMap[rpm] = build
				elif type(existing) is not set:
					rpmMap[rpm] = set((existing, build))
				else:
					existing.add(build)

		nAmbiguityErrors = 0
		for rpm, existing in rpmMap.items():
			if type(existing) is set:
				errormsg(f"{rpm} is generated by several builds: {' '.join(map(str, existing))}")
				if self.hints.checkBuildAlternatives(existing):
					infomsg(f"   accepted by the hints file")
					continue

				if len(existing) == 2:
					build = self.suggestBuildDisambiguation(rpm, *existing)
					if build is not None:
						infomsg(f"   will use {build} and hope that it will win")
						continue

				nAmbiguityErrors += 1

		return nAmbiguityErrors

	def suggestBuildDisambiguation(self, rpm, buildA, buildB):
		if buildA.name.startswith(buildB.name + ":") or rpm.name == buildA.name:
			return buildA

		if buildB.name.startswith(buildA.name + ":") or rpm.name == buildB.name:
			return buildB

		if ':' in buildA.name and ':' in buildB.name:
			baseBuildA, flavorA = buildA.name.split(':', maxsplit = 1)
			baseBuildB, flavorB = buildB.name.split(':', maxsplit = 1)

			if flavorA == flavorB:
				if len(baseBuildA) < len(baseBuildA):
					return buildA
				return buildB

		# hard-coded hack
		if buildA.name == 'SDL2' and buildB.name == 'sdl2-compat':
			return buildB
		if buildA.name == 'sdl2-compat' and buildB.name == 'SDL2':
			return buildA

		return None

	# We get here when an rpm has a dependency on a scenario (on at least one architecture),
	# but there is no single scenario common across all architectures.
	# This can happen due to a number of reasons
	#  fwupd: requires a kernel on x86_64 but not on aarch64
	#  podman: requires a kernel on all architectures, but s390x only has kernel-default.
	#	With s390x being unambiguous, we never went to check for a scenario, we just
	#	have a dependency on kernel-default.
	#
	# podman-like cases can be fixed by trying to find that lonely kernel-default dependency and
	# converting it to scenario kernel/image (valid for kernel=default).
	# The fwupd case is probably harder to handle.
	def scenarioDetective(self, db, genericRpm):
		def displayRpmDependencies(genericRpm, msg):
			if msg:
				infomsg(f"{genericRpm}: {msg}")
			infomsg(f"   common req {' '.join(map(str, genericRpm.solutions.common))}")
			for arch in genericRpm.architectures:
				specific = genericRpm.getDependencies(arch).difference(genericRpm.solutions.common)
				infomsg(f"   {arch} req {' '.join(map(str, specific))}")

				archScenarios = genericRpm.getScenarios(arch) or ["-"]
				infomsg(f"   {arch} scn {' '.join(map(str, archScenarios))}")

			infomsg(f"")

		if self.hints is None:
			return True

		if self.traceDisambiguation:
			infomsg(f"{genericRpm}: uses scenario(s) on one or more architectures, but I can't find a scenario valid across all architectures")

		if genericRpm.trace:
			displayRpmDependencies(genericRpm, "original dependencies")

		allRequiredScenarios = None
		for arch in genericRpm.architectures:
			reqScenarios = genericRpm.getScenarios(arch)
			if not reqScenarios:
				continue
			if allRequiredScenarios is None:
				allRequiredScenarios = reqScenarios
			else:
				allRequiredScenarios = allRequiredScenarios.intersection(reqScenarios)

		if not allRequiredScenarios:
			errormsg(f"{genericRpm}: different scenarios required on different architectures")
			return False

		for arch in genericRpm.architectures:
			if genericRpm.getScenarios(arch):
				continue

			archDependencies = genericRpm.getDependencies(arch)

			replace = []
			for requiredRpm in archDependencies:
				for concreteScenario in requiredRpm.getControllingScenarios(arch):
					if concreteScenario in allRequiredScenarios:
						replace.append((requiredRpm, concreteScenario))

			if replace:
				for (requiredRpm, concreteScenario) in replace:
					# concreteScenario is a string at this point, like kernel/default/image
					w = concreteScenario.split('/')
					assert(len(w) == 3)
					abstractPackageName = f"{w[0]}/{w[2]}"
					abstractRpm = db.createRpm(abstractPackageName, genericRpm.TYPE_SCENARIO)

					if genericRpm.trace:
						infomsg(f"{genericRpm}: replace {requiredRpm} with {concreteScenario} on {arch}")

					# Now replace the real dependency with the scenario package
					archDependencies.discard(requiredRpm)
					archDependencies.add(abstractRpm)

					genericRpm.addScenarios(arch, set((concreteScenario, )))

		genericRpm.validScenarios._common = None
		genericRpm.solutions._common = None

		if genericRpm.trace:
			displayRpmDependencies(genericRpm, "updated dependencies")

		if not genericRpm.validScenarios.common:
			return False

		if self.traceDisambiguation:
			infomsg(f"   {genericRpm}: successfully fixed up scenario dependencies")

		return True

##################################################################
# "patch" the code base by using the ghosts rpms to fudge
# unresolved dependencies
##################################################################
class PatchApplication(PreprocessApplicationBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def run(self):
		self.updateCodebasePatch()

	def displayUnresolvables(self, unresolvables):
		infomsg(f"Unresolvables:")
		fullArchSet = archRegistry.fullset

		for rpm in sorted(unresolvables, key = str):
			if rpm.architectures == fullArchSet:
				infomsg(f" - {rpm}")
			else:
				infomsg(f" - {rpm} [{rpm.architectures}]")

			map = {}
			for arch, depSet in rpm.unresolvables.items():
				for dep in depSet:
					if dep not in map:
						map[dep] = ArchSet()
					map[dep].add(arch)

			for dep, archSet in sorted(map.items()):
				if archSet == rpm.architectures:
					infomsg(f"    - {dep}")
				else:
					infomsg(f"    - {dep} [{archSet}]")
