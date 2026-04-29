##################################################################
#
# For a set of repositories, load the solv data and use use libsolv
# to resolve all package dependencies.
#
# We allow a certain degree of ambiguity, but disambiguate dependencies
# that cover eg different versions of java.
#
# Abstract across different architecures, and output a package DB
# that contains the generalized dependencies.
#
##################################################################

import solv
import os
import re
import functools

from .util import debugmsg, infomsg, warnmsg, errormsg, loggingFacade
from .util import ThatsProgress
from .arch import *
from .newdb import *
from .libsolv import *
from .scenario import *
from .rpmdeps import *
from .products import OBSNameFilter

__names__ = ['ArchSolver']

solvLogger = loggingFacade.getLogger('solver')
problemLogger = loggingFacade.getLogger('solverproblems')

def debugSolver(msg, *args, prefix = None, **kwargs):
        if prefix:
                msg = f"[{prefix}] {msg}"
        solvLogger.debug(msg, *args, **kwargs)

def debugSolverProblem(msg, *args, prefix = None, **kwargs):
        if prefix:
                msg = f"[{prefix}] {msg}"
        problemLogger.debug(msg, *args, **kwargs)

class RpmWrapper(RpmBase):
	isSourcePackage = False

	def __init__(self, name, arch, buildArch, solvable = None, type = None):
		super().__init__(f"{name}.{arch}", type)

		self.solvable = solvable
		self.buildArch = buildArch
		self.shortname = name
		self.buildName = None
		self.arch = arch

		self.abstractPackages = None
		self.controllingScenarios = None
		self.controllingScenarioVariables = None
		self.newControllingScenarios = None

		self.trace = False

		assert(arch not in ('src', 'nosrc'))

	# Helper functions for the scenario manager
	def initControllingScenario(self):
		if self.abstractPackages is None:
			self.abstractPackages = set()
			self.controllingScenarios = set()
			self.controllingScenarioVariables = set()
			self.newControllingScenarios = ConcreteScenarioSet()

	def addControllingScenario(self, abstractPackage, scenarioVersion):
		self.initControllingScenario()
		self.abstractPackages.add(abstractPackage)
		self.controllingScenarios.add(scenarioVersion)
		self.controllingScenarioVariables.add(abstractPackage.scenarioVar)

	def addControllingScenarioNew(self, concreteScenario):
		self.initControllingScenario()
		self.newControllingScenarios.add(concreteScenario)
		if not concreteScenario.control.isComplete:
			raise Exception(f"{self}: trying to add invalid controlling scenario {concreteScenario}")

	# Helper function for the scenario manager
	def extractVersion(self, versionFormat = '{major}'):
		assert(versionFormat == '{major}')

		# For now, we always extract the major from the EVR string.
		# If we ever need anything more flexible (eg major.minor for python),
		# the hints.conf syntax needs to be extended.
		version = self.solvable.evr.split('-')[0]
		version = version.split('.')
		major = version[0]

		return major


class RpmFactory(object):
	def __init__(self, buildArch):
		self.buildArch = buildArch
		self._nameToRpm = {}
		self._shortNameToRpm = {}
		self._idToRpm = {}

		self._byType = {}
		for type in RpmBase.VALID_TYPES:
			self._byType[type] = set()

		self.traceMatcher = None

	def newRpm(self, *args, **kwargs):
		rpm = RpmWrapper(*args, **kwargs)

		if self.traceMatcher is not None and rpm.arch not in ('src', 'nosrc'):
			rpm.trace = self.traceMatcher.match(rpm.shortname)
			if rpm.trace:
				infomsg(f"{rpm}: tracing enabled")

		self._nameToRpm[rpm.name] = rpm
		self._shortNameToRpm[rpm.shortname] = rpm
		self._byType[rpm.type].add(rpm)
		if rpm.solvable is not None:
			self._idToRpm[rpm.solvable.id] = rpm

		return rpm

	def createFromSolvable(self, solvable, type = None):
		rpm = self._idToRpm.get(solvable.id)
		if rpm is not None:
			# When dealing with stagings, it happens all the time that we see rpms with the same name but
			# different solvables attached.
			if False:
				if rpm is not self._shortNameToRpm[solvable.name]:
					other = self._shortNameToRpm[solvable.name]
					raise Exception(f"Tried to look up solvable {solvable} but found conflicting {other.solvable}")

			if type is not None and rpm.type != type:
				raise Exception(f"{rpm}: cannot change type from {rpm.type} to {type}")
			return rpm

		if solvable.name.startswith('pattern:') or solvable.name.startswith('product:'):
			assert(type is None)
			type = RpmWrapper.TYPE_METAPKG

		return self.newRpm(solvable.name, solvable.arch, self.buildArch, solvable = solvable, type = type)

	def getByName(self, name):
		rpm = self._nameToRpm.get(name)
		if rpm is None:
			rpm = self._shortNameToRpm.get(name)
		return rpm

	def createDummyRpm(self, name, type):
		rpm = self.getByName(name)
		if rpm is None:
			rpm = self.newRpm(name, 'noarch', self.buildArch, type = type)
		elif rpm.type != type:
			raise Exception(f"Cannot change {rpm} from type {rpm.type} to {type}")

		return rpm

	def getAllByType(self, type):
		return self._byType[type]

class ArchSolver(object):
	def __init__(self, arch, ignoreConflicts = False, hints = None, traceMatcher = None, errorReport = None):
		self.arch = arch
		self.pool = solv.Pool()
		self.pool.setarch(arch)
		self.resolverLog = None
		self.errorReport = errorReport

		self.pedantic = False
		self.traceDisambiguation = False

		self.rpmFactory = RpmFactory(self.arch)
		self._rpms = []

		if traceMatcher:
			self.rpmFactory.traceMatcher = traceMatcher

		self.hints = hints
		self.dependencyOracle = None

		self.ignoreConflicts = ignoreConflicts
		self.useRecommends = False
		self.alwaysFavored = set()

		self._vendor = None

		self.repoCount = 0
		self._dummyRepo = self.pool.add_repo('Package:Monkey:Dummy')
		self._resolvedDependencies = []
		self.resolvedRpms = []
		self.unresolvableRpms = []

		self.queue = []

	def addRepository(self, repository):
		repo = self.pool.add_repo(repository.projectName)

		solverOutputFile = repository.solverDataPath
		if not os.path.isfile(solverOutputFile):
			raise Exception(f"{repository}: no solver output file; did you download the repository first?")

		# FIXME: stat the file and issue a warning if it is older than a few days

		if not repo.add_solv(solverOutputFile):
			raise Exception(f'{repository}: failed to add solver file {solverOutputFile}')

		infomsg(f"{repository}: inspecting set of packages")
		self.repoCount += 1

		for solvable in repo.solvables_iter():
			if self.ignoreConflicts:
				solvable.unset(solv.SOLVABLE_CONFLICTS)
				solvable.unset(solv.SOLVABLE_OBSOLETES)

			if self._vendor is None:
				self._vendor = solvable.vendor

			if self.hints and self.hints.ignorePackageName(solvable.name):
				continue

			rpm = self.solvableToRpm(solvable)
			self._rpms.append(rpm)
			self.queue.append(rpm)

	# We must apply the resolver hints after adding all repos.
	def applyHints(self):
		hints = self.hints
		if hints is None:
			return

		for name in hints.syntheticNames:
			self.createDummySolvable(name, type = RpmWrapper.TYPE_SYNTHETIC)

		for name in hints.preferredNames:
			rpm = self.nameToRpm(name)
			if rpm is None:
				raise Exception(f"{self}: resolver hints tells us to prefer {name}, but I cannot find this package")
			self.alwaysFavored.add(rpm)

		self.unresolvableRpm = self.createDummySolvable('__unresolved__', type = RpmWrapper.TYPE_SYNTHETIC)
		PackageDependencies.unresolvableRpm = self.unresolvableRpm

		for name in hints.knownMissingNames:
			params = name.split(';')
			name = params.pop(0)

			if self.nameToRpm(name) is not None:
				# it's not missing after all
				continue

			kwargs = {}
			for p in params:
				key, value = p.split('=')
				if key == 'version':
					kwargs['evr'] = value
				elif key == 'arch':
					kwargs['arch'] = value
				elif key == 'like':
					likeRpm = self.nameToRpm(value)
					if likeRpm is None:
						errormsg(f"Missing package {name}: cannot copy version/arch from {value} because {value} does not exist on {self.arch}")
					else:
						kwargs['evr'] = likeRpm.solvable.evr
						kwargs['arch'] = likeRpm.solvable.arch
				else:
					errormsg(f"Missing package {name}: ignoring unsupported parameter {p}")

			self.createDummySolvable(name, type = RpmWrapper.TYPE_MISSING, **kwargs)

		self._dummyRepo.internalize()

		self.dependencyOracle = DependencyOracle(hints, self.pool)
		hints.rebind(self.rpmFactory)

		# This needs to be configurable via hints:
		abiProviderKeys = (
			'python(abi)',
			'golang(API)',
		)
		self.abiManager = AbiManager(abiProviderKeys)

	def solvableToRpm(self, solvable, type = None):
		return self.rpmFactory.createFromSolvable(solvable, type)

	def solvableSetToRpms(self, solvableSet):
		return set(map(self.solvableToRpm, solvableSet))
	
	def nameToRpm(self, name):
		return self.rpmFactory.getByName(name)

	def getAllRpms(self, type):
		return self.rpmFactory.getAllByType(type)

	def createDummySolvable(self, name, evr = "0.0-1", arch = "noarch", provides = None, type = None):
		if type is None:
			raise Exception(f"You have to specify a type when creating dummy rpm {name}")

		solvable = self._dummyRepo.add_solvable()
		solvable.name = name
		solvable.evr = evr
		solvable.arch = arch
		solvable.vendor = self._vendor

		# Make the solvable provide itself so that select() sees it
		if provides is None:	
			provides = name

		dep = self.pool.Dep(provides, 1)
		solvable.add_deparray(solv.SOLVABLE_PROVIDES, dep)

		# create the rpm and override the type
		rpm = self.solvableToRpm(solvable, type)
		assert(type is None or type == rpm.type)

		if rpm.trace:
			infomsg(f"Created synthetic solvable {name}; type={type}")

		self._rpms.append(rpm)
		return rpm

	def createDummyRpm(self, name, type = False):
		if type is None:
			raise Exception(f"You have to specify a type when creating dummy rpm {name}")

		rpm = self.rpmFactory.createDummyRpm(name, type = type)
		self._rpms.append(rpm)

		if rpm.trace:
			infomsg(f"Created dummy rpm {name}; type={type}")

		return rpm

	def createScenarioRpm(self, name):
		return self.createDummyRpm(name, RpmWrapper.TYPE_SCENARIO)

	# solve some or all rpms in a set of repositories.
	def solve(self, progressMeter, rpms = None, db = None, **kwargs):
		self.applyHints()
		self.pool.addfileprovides()
		self.pool.createwhatprovides()

		if rpms is None:
			rpms = self._rpms

		if db is not None:
			for rpm in rpms:
				if rpm.isSynthetic:
					continue
				genericRpm = db.lookupRpm(rpm.shortname)
				if genericRpm is not None and genericRpm.new_build is not None:
					rpm.buildName = genericRpm.new_build.name

		totalCount = len(rpms)

		infomsg(f"{self.arch}: solving {totalCount} rpms")

		# we need to check all RPMs for ABI providers, not just those that we
		# want to resolve.
		infomsg(f"Looking for ABI providers")
		for rpm in self._rpms:
			if rpm.isSynthetic:
				continue

			self.detectAbiProviders(rpm)

		if self.rpmFactory.traceMatcher is not None:
			prefer = []
			other = []

			for rpm in rpms:
				if rpm.trace:
					prefer.append(rpm)
				else:
					other.append(rpm)
			rpms = prefer + other

		for rpm in rpms:
			if rpm.isSynthetic:
				continue

			with loggingFacade.temporaryIndent():
				self.tryToSolveRpm(rpm)

			if progressMeter is not None:
				progressMeter.tick()
				if progressMeter.count % 100 == 0:
					infomsg(f"{progressMeter} {self.arch} {rpm.shortname}")

		if self.unresolvableRpms:
			unresolvedCount = len(self.unresolvableRpms)
			infomsg(f"Resolved {totalCount - unresolvedCount}/{totalCount} rpms; {unresolvedCount} unresolvable")
		else:
			infomsg(f"Resolved all {totalCount} rpms")

	# try to resolve one RPM on one architecture
	def tryToSolveRpm(self, rpm):
		result = self.resolveAndDetectAmbiguities(rpm)
		self._resolvedDependencies.append(result)

		installRequest = self.InstallationRequest(self.pool, rpm)
		if not result.isResolvable:
			resolved = result
		elif result.isAmbiguous:
			# replace ambiguous resolutions with symbolic rpms and
			# record valid choices
			resolved = self.disambiguate(rpm, result)
			if resolved is None:
				self.reportDisambiguationFailure(result, self.errorReport)
		elif not self.pedantic:
			resolved = result
		else:
			resolved = self.resolveOnePackage(installRequest, result)

		if not resolved:
			errormsg(f"{rpm}: unable to resolve dependencies")
			self.resolverLog.logUnresolvablePackage(result, installRequest.problems)
			self.unresolvableRpms.append(rpm)
			return

		self.resolvedRpms.append(resolved)

		if self.resolverLog is not None:
			self.resolverLog.logResolvedPackage(resolved)

	def detectAbiProviders(self, rpm):
		sel = self.pool.select(rpm.shortname, solv.Selection.SELECTION_NAME)
		if sel.isempty():
			infomsg(f"Error: {rpm} not found")
			return None

		for solvable in self.disambiguateStaging(sel.solvables()):
			for dep in solvable.lookup_deparray(solv.SOLVABLE_PROVIDES):
				abi = self.abiManager.dependencyToAbi(dep)
				if abi is not None:
					self.abiManager.addProvider(abi, solvable)

	def checkAbiCompatibility(self, rpm, abiCompatibility):
		solvable = rpm.solvable

		for dep in solvable.lookup_deparray(solv.SOLVABLE_REQUIRES):
			abi = self.abiManager.dependencyToAbi(dep)
			if abiCompatibility.conflicts(abi):
				# infomsg(f"{rpm}: requires {abi}, which conflicts {abiCompatibility.conflicts(abi)}")
				return False
		return True

	def resolveAndDetectAmbiguities(self, rpm, key = 'requires'):
		sel = self.pool.select(rpm.shortname, solv.Selection.SELECTION_NAME)
		if sel.isempty():
			infomsg(f"Error: {rpm} not found")
			return None

		if key == 'requires':
			dependencyType = solv.SOLVABLE_REQUIRES
		elif key == 'recommends':
			dependencyType = solv.SOLVABLE_RECOMMENDS
		else:
			fail

		if rpm.trace:
			infomsg(f"Resolving provides for {rpm}")

		result = PackageDependencies(rpm, key)

		for s in self.disambiguateStaging(sel.solvables()):
			providedIds = set()
			provided = set()
			for dep in s.lookup_deparray(solv.SOLVABLE_PROVIDES):
				providedIds.add(dep.id)
				provided.add(str(dep))

			for dep in s.lookup_deparray(dependencyType):
				abi = self.abiManager.dependencyToAbi(dep)
				if abi is not None:
					if rpm.trace:
						infomsg(f"   {key} {dep} -> abi {abi}")
					result.abiCompatibility.add(abi)

			for dep in s.lookup_deparray(dependencyType):
				rd = None

				if rpm.trace:
					infomsg(f"   requires {dep}")

				conditionals = self.tryProcessConditional(rpm, dep)
				if conditionals is not None:
					for node in conditionals:
						result.addConditional(dep, str(node))
					continue

				choices = self.dependencyToSelection(rpm, dep)
				if choices is None:
					if rpm.trace:
						infomsg(f"      ignored")
					continue

				if not choices:
					if rpm.trace:
						infomsg(f"      resolved to nothing")
					result.markUnresolvable(dep)
					continue

				if rpm in choices:
					if rpm.trace:
						infomsg(f"      resolved to self")
					# Note: we do not want to track dependencies of a package on itself.
					# It's superfluous, plus it creates issues when dealing with scenarios.
					continue

				if rpm.trace:
					infomsg(f"      resolved to {' '.join(map(str, choices))}")
				elif any(req.trace for req in choices):
					infomsg(f"{rpm}: {dep} requires {' '.join(map(str, choices))}")

				origChoices = choices

				# Use python(abi) to disambiguate.
				# For instance, aws-cli may require "python(abi) = 313", and
				# some "python3-gobject". The latter has two possible solutions,
				# python311-gobject and python313-gobject. Both of them have a
				# dependency on "python(abi) = ...", so we use that to pick the
				# correct python313-gobject.
				if result.abiCompatibility:
					choices = set(filter(lambda rpm: self.checkAbiCompatibility(rpm, result.abiCompatibility), choices))
					if not choices:
						errormsg(f"{rpm}/{dep}: no candidate that is compatible with required ABI(s) {result.abiCompatibility}")
						result.markUnresolvable(dep)
						continue
					if rpm.trace:
						infomsg(f"      abi compat transformed to {' '.join(map(str, choices))}")

				choices = self.filterAlternatives(choices)
				if not choices:
					errormsg(f"{rpm}/{dep}: filterAlternatives failed?!")
					result.markUnresolvable(dep)
					continue

				if rpm.trace and choices != origChoices:
					infomsg(f"      filter transformed to {' '.join(map(str, choices))}")

				suppress = origChoices.difference(choices)
				if rpm.trace and suppress:
					infomsg(f"      disfavor {' '.join(map(str, suppress))}")
				result.disfavoredRpms.update(suppress)

				if len(choices) == 1:
					solution = next(iter(choices))
					result.addSolution(dep, solution)

					abi = self.abiManager.getAbi(solution.solvable)
					if abi is not None:
						result.abiCompatibility.add(abi)

					if rpm.trace:
						if abi is None:
							infomsg(f"      unique solution {solution}")
						else:
							infomsg(f"      unique solution {solution}; abi={abi}")
					continue

				acceptable = self.hints.areAlternativesAcceptable(choices)
				result.addAmbiguousSolution(dep, choices, acceptable)

				if rpm.trace:
					if acceptable:
						infomsg(f"      accepted solution {' '.join(map(str, choices))}")
					else:
						infomsg(f"      ambiguous solution {' '.join(map(str, choices))}")
				elif any(req.trace for req in choices):
					infomsg(f"{rpm}: {dep} requires {' '.join(map(str, choices))}")

		# Handle the second type of abi compatibility checks:
		# pkg foo:
		#	requires /usr/bin/python3.13
		#		-> python313-base (which provides python(abi) = 3.13
		#	requires python3-gobject
		#		-> python313-gobject python311-gobject
		# We can disambiguate these by looking at the required abi of python*-gobject:
		if result.abiCompatibility:
			for rd in result:
				if not rd.requiresDisambiguation:
					continue

				choices = set(filter(lambda rpm: self.checkAbiCompatibility(rpm, result.abiCompatibility), rd.alternatives))
				if len(choices) == 1:
					rd.solutions = choices
					rd.alternatives = set()
					if rpm.trace:
						infomsg(f"      unique solution {solution}")
					assert(not rd.requiresDisambiguation)

		# Handle a common case that's easy to disambiguate.
		# Example: many packages require something like typelib(Gdk) and 'typelib(Gtk) = 3.0'.
		# The latter has a unique solution (typelib-Gtk-3.0), whereas the former is ambiguous, and
		# could be solved using typelib-Gtk-1.0, typelib-Gtk-2.0, ...
		# A similar case applies for KMPs.
		# We disambiguate by checking whether any ambiguous dependency A would be resolved by
		# one of the packages we're pulling in anyway (through another dependency B). If that's
		# the case, then dependency A is already satisfied by the solution to B, and there is
		# no real ambiguity.
		if result.isAmbiguous:
			uniqueSolutions = set()
			for rd in result:
				uniqueSolutions.update(rd.solutions)

			for rd in result:
				if not rd.requiresDisambiguation:
					continue

				selectedAlready = rd.alternatives.intersection(uniqueSolutions)
				if selectedAlready:
					rd.solutions.update(selectedAlready)
					rd.alternatives = set()

		return result

	def tryProcessConditional(self, rpm, dep):
		depString = str(dep)
		if ' if ' not in depString and ' unless ' not in depString:
			return None

		try:
			node = BooleanDependency.parse(depString, self.dependencyOracle)
		except Exception as e:
			errormsg(f"{rpm}: could not parse conditional dependency \"{depString}\" (exception {e})")
			return None

		implications = list(node.implications())

		if rpm.trace:
			infomsg(f"      compiled as {node}")
			if len(implications) == 1 and str(implications[0]) == str(node):
				pass
			elif not implications:
				infomsg(f"      never expands to any requirements (within this codebase); ignore it")
			else:
				infomsg(f"      transformed to the following requirement(s):")
				for k in implications:
					infomsg(f"         -> {k}")

		return implications

	def dependencyToSelection(self, rpm, dep):
		# transform the dependency string if there is a rule for it
		newString = self.hints.transformDependency(str(dep), rpm.shortname)
		if newString is not None:
			if rpm.trace:
				infomsg(f"   transformed into {newString}")
			dep = self.pool.Dep(newString, 1)
			assert(dep is not None)

		choices = set(self.pool.whatprovides(dep.id))
		if choices and all(solvable.name.startswith('system:') for solvable in choices):
			return None

		if self.repoCount > 1:
			choices = self.disambiguateStaging(choices)

		return self.solvableSetToRpms(choices)

	# When using the packages from a staging project on top of the existing build project,
	# we constantly encounter two rpms with the same name.
	# Disambiguate by doing a name lookup. This will return the last rpm with that name to be
	# defined (which should be the one from Staging if it exists; and the original one otherwise.
	def disambiguateStaging(self, solvableSet):
		result = set()
		for solvable in solvableSet:
			rpm = self.rpmFactory.getByName(solvable.name)
			assert(rpm.solvable is not None)
			result.add(rpm.solvable)

		return result

	class InstallationRequest(object):
		def __init__(self, pool, installRpm, scenarioVersion = None, useRecommends = False):
			self.pool = pool
			self.mainRpm = installRpm
			self.name = installRpm.name
			self.scenarioVersion = scenarioVersion
			self.useRecommends = useRecommends

			self.installRpms = set()
			self.favoredRpms = set()
			self.disfavoredRpms = set()

			self.requestedSolvables = set()
			self.requestedSolvables.add(str(installRpm.solvable))

			self.addRpm(installRpm)

			self.trace = installRpm.trace
			self.problems = []

		def __str__(self):
			if self.scenarioVersion:
				return f"{self.name} with {self.scenarioVersion}"
			return self.name

		def addRpm(self, rpm):
			self.installRpms.add(rpm)

		def addFavoredRpm(self, rpm):
			self.favoredRpms.add(rpm)

		class Transaction(object):
			def __init__(self, solver, jobs):
				self.solver = solver
				self.problems = solver.solve(jobs)

				trans = solver.transaction()
				self.isEmpty = trans.isempty()

				self.solutions = []
				for s in trans.newsolvables():
					debugSolver(f"   -> {s}")
					reason, rule = solver.describe_decision(s)

					debugSolver(f"      rule: {rule.id} {rule.info()}")
					self.solutions.append((s, rule))

				self.alternatives = list(solver.alternatives())

		def transact(self):
			if self.trace:
				infomsg(f"{self.mainRpm} building transaction")
			solver = self.pool.Solver()

			solver.set_flag(solver.SOLVER_FLAG_IGNORE_RECOMMENDED, not self.useRecommends)
			solver.set_flag(solver.SOLVER_FLAG_ADD_ALREADY_RECOMMENDED, self.useRecommends)

			jobs = []
			for rpm in self.installRpms:
				sel = self.pool.select(rpm.shortname, solv.Selection.SELECTION_NAME)
				if sel.isempty():
					raise Exception(f"Cannot install {rpm}: not found")

				if self.trace:
					infomsg(f"   install {rpm}")
				jobs += sel.jobs(solv.Job.SOLVER_INSTALL)

			for rpm in self.favoredRpms:
				sel = self.pool.select(rpm.shortname, solv.Selection.SELECTION_NAME)
				if sel.isempty():
					raise Exception(f"Cannot favor {rpm}: not found")

				if self.trace:
					infomsg(f"   favor {rpm}")
				jobs += sel.jobs(solv.Job.SOLVER_FAVOR)

			for rpm in self.disfavoredRpms:
				sel = self.pool.select(rpm.shortname, solv.Selection.SELECTION_NAME)
				if sel.isempty():
					raise Exception(f"Cannot favor {rpm}: not found")

				if self.trace:
					infomsg(f"   disfavor {rpm}")
				jobs += sel.jobs(solv.Job.SOLVER_DISFAVOR)

			return self.Transaction(solver, jobs)

		def isDirectDependency(self, rule):
			for ri in rule.allinfos():
				if str(ri.solvable) in self.requestedSolvables:
					return True
			return False

		def createProblem(self):
			problem = Problem()
			self.problems.append(problem)
			return problem

		def displayProblems(self):
			infomsg(f"{self}: {len(self.problems)} problem(s) encountered while trying to resolve dependencies:")
			for problem in self.problems:
				indent = "  "
				for msg in problem.infoMessages:
					infomsg(f"{indent}{msg}")

				for item in problem:
					infomsg(f"{indent} -> {item}")
					indent += "  "

		def hasConflicts(self):
			return any(problem.isConflict for problem in self.problems)

		@property
		def unresolvedDependencies(self):
			result = set()
			for problem in self.problems:
				result.update(problem.unresolvedDependencies)
			return result

	def disambiguate(self, rpm, ambiguousResult):
		trace = self.traceDisambiguation or rpm.trace

		disambiguation = self.processAmbiguities(ambiguousResult)
		if disambiguation is None:
			errormsg(f"Cannot disambiguate {rpm}")
			return None

		validFor = self.disambiguateOnePackage(rpm, disambiguation)
		if not validFor:
			errormsg(f"Cannot disambiguate {rpm}")
			ambiguousResult.failedAlternatives = disambiguation.failedAlternatives
			return None

		commonVersion = validFor.commonVersion
		if commonVersion:
			if trace:
				infomsg(f"{rpm}: unique disambiguation using version {commonVersion}")

			for rd in ambiguousResult:
				if not rd.requiresDisambiguation:
					continue

				rpms = set()
				for solution in validFor:
					rpms.update(solution.selectedRpms)
				rpms.intersection_update(rd.alternatives)
				assert(rpms)

				if trace:
					infomsg(f"  {rd}: replace with {' '.join(map(str, rpms))}")

				rd.solutions = rpms
				rd.acceptableAmbiguity = True
			return ambiguousResult

		# replace ambiguous resolutions with symbolic rpms and
		# record valid choices
		for rd in ambiguousResult:
			if not rd.requiresDisambiguation:
				continue

			symbolicRpmNames = disambiguation.getSymbolicRpms(rd)
			rpms = set(map(self.createScenarioRpm, symbolicRpmNames))
			assert(all(rpms))

			if trace:
				infomsg(f"  {rd}: replace with {' '.join(map(str, rpms))}, {' '.join(map(str, validFor))}")

			rd.solutions = rpms
			rd.acceptableAmbiguity = True

		ambiguousResult.validScenarioChoices = set(map(str, validFor))
		ambiguousResult.controllingScenarioChoices = rpm.controllingScenarios
		return ambiguousResult

	def disambiguateOnePackage(self, installRpm, disambiguation):
		trace = self.traceDisambiguation or installRpm.trace

		verifiedScenarios = disambiguation.createEmptySubset()
		for solution in disambiguation:
			installRequest = self.InstallationRequest(self.pool, installRpm, scenarioVersion = solution.selectedVersions)

			for rpm in solution.selectedRpms:
				installRequest.addRpm(rpm)
			if trace:
				infomsg(f"Disambiguate {installRequest}: {' '.join(map(str, solution.selectedRpms))}")

			with loggingFacade.temporaryIndent():
				result = self.resolveOnePackageWork(installRequest)

			if result is None:
				disambiguation.failedAlternatives.append(installRequest)

				if trace:
					infomsg(f"Trouble with {installRequest}:")
					for problem in installRequest.problems:
						for item in problem:
							infomsg(f"{item}")
							if isinstance(item, problem.NothingProvides):
								infomsg(f"  NothingProvides dep={item.dep}")
							elif isinstance(item, problem.Requires):
								infomsg(f"  Requires dep={item.dep} rpm={item.rpm}")
							elif isinstance(item, problem.Conflict):
								infomsg(f"  Conflict rpms={' '.join(map(str, item.rpms))}")
							else:
								infomsg(f"  Unexpected rpm install problem of type {type(item)}")

					infomsg(f"{installRpm}: unresolvable conflict in {solution.selectedVersions}")
					installRequest.displayProblems()
				continue

			if result.isAmbiguous:
				errormsg(f"Duh, {result} is still ambiguous after disambiguation ({solution.selectedVersions})")
				result.displayAmbiguities()
				continue

			if trace:
				infomsg(f"  ok: successfully disambiguated {solution}")
			verifiedScenarios.add(solution)

		return verifiedScenarios

	def resolveOnePackage(self, installRequest, partiallyResolved):
		# The preceding step performed a "whatprovides" resolution. Where
		# that yielded an ambiguous solution (eg /bin/sh being provided by 4 different rpms)
		# we tried to whittle that down to a single solution.
		# Add those to the installRequest to make sure we're not getting random
		# packages as resolution in this step.
		for rq in partiallyResolved:
			# favoring things like systemd over systemd-mini does not seem to work :-(
			installRequest.installRpms.update(rq.solutions)
			# FIXME fake the dependeny on output

		installRequest.disfavoredRpms.update(partiallyResolved.disfavoredRpms)

		# The hints may tell us to always prefer certain packages.
		# For example, when the package has scripts, it seems that libsolv will always
		# ask for '/bin/sh'. However, if the package itself does not have an explicit
		# dependency on /bin/sh, the resolver will do whatever it likes, and we usually end up
		# with a dependency on bash-legacybin rather than bash-sh
		installRequest.favoredRpms.update(self.alwaysFavored)

		result = self.resolveOnePackageWork(installRequest)
		if result is None:
			if not installRequest.problems:
				errormsg(f"{installRequest}: unresolvable, but no list of problems")
			else:
				installRequest.displayProblems()

			errormsg(f"{installRequest}: cannot resolve")
			return None

		if result.isAmbiguous:
			errormsg(f"{installRequest}: unexpected ambiguity while resolving package dependencies")
			return None

		return result

	def resolveOnePackageWork(self, installRequest):
		# solve the install request
		transaction = installRequest.transact()

		if transaction.problems:
			for prob in transaction.problems:
				self.processProblem(installRequest, prob)
			return None

		if transaction.isEmpty:
			errormsg(f"{installRequest}: nothing to do.")
			return None

		if installRequest.trace:
			infomsg(f"   resolved {' '.join(installRequest.requestedSolvables)}")

		result = PackageResolution(installRequest.mainRpm, scenarioVersion = installRequest.scenarioVersion)
		for s, rule in transaction.solutions:
			if installRequest.isDirectDependency(rule):
				if installRequest.trace:
					infomsg(f"   direct {rule}: {s}")
				rpm = self.solvableToRpm(s)
				result.addResolved(rule.info().dep, rpm)
			elif installRequest.trace:
				infomsg(f"   ignoring indirect {rule}: {s}")

		for alt in transaction.alternatives:
			choices = self.solvableSetToRpms(alt.choices())
			choices = self.filterAlternatives(choices)

			if len(choices) > 1:
				rd = result.getResolved(alt.rule.info().dep)
				if rd is None:
					# only an indirect dependency; don't bother
					continue

				# In some cases, we may not want to disambiguate the alteratives and
				# just accept them.
				acceptable = False
				if self.hints:
					acceptable = self.hints.areAlternativesAcceptable(choices)
					if acceptable:
						debugSolver(f"{installRequest}: {' '.join(map(str, choices))} are acceptable")

				if self.traceDisambiguation:
					infomsg(f"   {alt.rule.info().dep} is ambiguous: {' '.join(map(str, choices))}")
				rd.addAlternatives(choices, acceptable = acceptable)

		return result

	def filterAlternatives(self, choices):
		if len(choices) <= 1 or self.hints is None:
			return choices

		# FIXME: we could catch quite a few trivial ambiguities by checking for
		# mutual dependencies. E.g. we have packages foo-devel, libfoo0 where
		# foo-devel also provides libfoo.so.0. Therefore, whenever something needs
		# libfoo.so.0, libsolv will return both libfoo0 and foo-devel. We can
		# usually catch these easily because foo-devel will also require libfoo0...

		return self.hints.filterChoices(choices)

	class ScenarioDisambiguation(object):
		def __init__(self, scenario):
			self.scenario = scenario

			self._map = {}
			self.symbolicRpms = set()
			self.failedAlternatives = []

		def add(self, scenarioVersion, rpms):
			if scenarioVersion not in self._map:
				self._map[scenarioVersion] = set()
			self._map[scenarioVersion].update(rpms)

		def items(self):
			return self._map.items()

	def reportDisambiguationFailure(self, result, errorReport):
		if errorReport is None:
			return

		rpm = result.requiringPkg

		errorReport.add(f"{rpm}: unable to disambiguate")
		scenarioVariables = set()
		for rd in result:
			if not rd.requiresDisambiguation:
				continue

			if rd.abstractScenarioPackage is not None:
				scenarioVariables.add(rd.abstractScenarioPackage.scenarioVar)
			else:
				# Do not report each and every dependency with alternatives; just report the one
				# that is not covered by a scenario
				lacking = rd.alternativesWithoutScenario()
				if lacking:
					errorReport.add(f"   {rd}: no scenario for {' '.join(sorted(map(str, lacking)))}")

		if len(scenarioVariables) > 1:
			errorReport.add(f"   using packages from multiple scenarios {' '.join(map(str, scenarioVariables))}: currently not supported")
			return

		if result.failedAlternatives:
			for installRequest in result.failedAlternatives:
				# Note: installRequest.problems is a list of Problem objects; each problem
				# can be iterated over, with items being
				#   a) instance of problem.NothingProvides
				#	.rpm is the requiring rpm, .dep is the dependency string
				#   b) a pair of instances of problem.Requires (.rpm and .dep like above),
				#	and problem.Conflict, the latter having a .rpms member that is
				#	a set of packages that conflict.
				errorReport.add(f"   Failed alternative {installRequest}")
				for problem in installRequest.problems:
					indent = "  "
					for item in problem:
						errorReport.add(f"{indent} -> {item}")
						indent += "  "

	def processAmbiguities(self, result):
		if self.hints is None:
			return None

		trace = self.traceDisambiguation or result.requiringPkg.trace

		if trace:
			infomsg(f"Need to process ambiguous dependencies of {result} ctl={result.requiringPkg.newControllingScenarios}")

		if not result.canAttemptScenarioDisambiguation(verbose = True):
			infomsg(f"{result}: cannot perform scenario-based disambiguation")
			return None

		salad = ScenarioSalad(result.requiringPkg.shortname,
					controllingScenarios = result.requiringPkg.newControllingScenarios,
					trace = trace)

		for rd in result:
			if rd.requiresDisambiguation:
				if not salad.add(rd, rd.alternatives):
					return None

		with loggingFacade.temporaryIndent():
			scenarioSolutions = salad.solve()

		if trace:
			infomsg(f"solutions: {' '.join(map(str, scenarioSolutions))}")

		# We need to add this member for later
		scenarioSolutions.failedAlternatives = []

		return scenarioSolutions

	def processProblem(self, installRequest, prob):
		# If you cook problems long enough, they will dissolve
		cookedProblem = installRequest.createProblem()

		cookedProblem.addInfoMessage(f"{installRequest}: {prob}")

		for rule in prob.findallproblemrules():
			debugSolverProblem(f"  {rule} type={rule.type}")

			types = set(ri.type for ri in rule.allinfos())
			if len(types) == 1:
				type = types.pop()
			else:
				infomsg(f"    Cannot handle rule that mixes different ruleinfo types")
				type = None

			if type == solv.Solver.SOLVER_RULE_PKG_REQUIRES:
				for ri in rule.allinfos():
					debugSolverProblem(f"      {ri} {ri.othersolvable}")
					cookedProblem.addRequires(self.solvableToRpm(ri.solvable), str(ri.dep))
			elif type == solv.Solver.SOLVER_RULE_PKG_OBSOLETES:
				for ri in rule.allinfos():
					debugSolverProblem(f"      {ri} {ri.othersolvable}")
					cookedProblem.addObsoletes(self.solvableToRpm(ri.solvable), str(ri.dep))
			elif type == solv.Solver.SOLVER_RULE_PKG_CONFLICTS:
				conflicting = set()
				for ri in rule.allinfos():
					debugSolverProblem(f"      {ri}")

					assert(ri.othersolvable is not None)
					rpm = self.solvableToRpm(ri.othersolvable)
					conflicting.add(rpm)

				cookedProblem.addConflict(conflicting)
				debugSolverProblem(f"    Conflict: {' '.join(map(str, conflicting))}")
			elif type == solv.Solver.SOLVER_RULE_PKG_NOTHING_PROVIDES_DEP:
				for ri in rule.allinfos():
					debugSolverProblem(f"       {ri.solvable} {ri.dep}")
					cookedProblem.addNothingProvides(self.solvableToRpm(ri.solvable), str(ri.dep))
			elif type is not None:
				infomsg(f"Need to handle type {type}")

				for ri in rule.allinfos():
					infomsg(f"    [{ri.type}] {ri}")
					if ri.othersolvable is not None:
						infomsg(f"      othersolvable={ri.othersolvable}")

		return cookedProblem

class Problem(object):
	class Requires(object):
		def __init__(self, rpm, dep):
			self.rpm = rpm
			self.dep = dep

		def __str__(self):
			return f"{self.rpm} requires {self.dep}"

	class Obsoletes(Requires):
		def __str__(self):
			return f"{self.rpm} obsoletes {self.dep}"

	class NothingProvides(Requires):
		def __str__(self):
			return f"{self.rpm} has unsatisified requirement {self.dep}"

	class Conflict(object):
		def __init__(self, rpms):
			self.rpms = rpms

		def __str__(self):
			return f"conflict {' '.join(map(str, self.rpms))}"

	def __init__(self):
		self._info = []
		self._chain = []

	def addInfoMessage(self, m):
		self._info.append(m)

	@property
	def infoMessages(self):
		return self._info

	def addRequires(self, *args):
		self._chain.append(self.Requires(*args))

	def addObsoletes(self, *args):
		self._chain.append(self.Obsoletes(*args))

	def addConflict(self, *args):
		self._chain.append(self.Conflict(*args))

	def addNothingProvides(self, *args):
		self._chain.append(self.NothingProvides(*args))

	def __iter__(self):
		return iter(self._chain)

	@property
	def isConflict(self):
		return any(isinstance(item, self.Conflict) for item in self._chain)

	@property
	def unresolvedDependencies(self):
		result = set()
		for item in self._chain:
			if isinstance(item, self.NothingProvides):
				result.add(item.dep)
		return result


class ResolvedDependency(object):
	def __init__(self, dep, solution = None):
		self.dep = dep
		self.solutions = set()
		self.alternatives = set()
		self.acceptableAmbiguity = True
		self.abstractScenarioPackage = None

		if solution is not None:
			self.solutions.add(solution)

	def __str__(self):
		return str(self.dep)

	def addAlternative(self, rpm, acceptable = False):
		if not acceptable:
			self.acceptableAmbiguity = False

		self.alternatives.add(rpm)

	def addAlternatives(self, rpms, acceptable):
		self.alternatives.update(rpms)

		if not acceptable:
			self.acceptableAmbiguity = False

	@property
	def requiresDisambiguation(self):
		return len(self.alternatives) > 1 and not self.acceptableAmbiguity

	def findAlternative(self, rpmName):
		for name in self.alternatives:
			if name == rpm:
				return name

		for name in self.alternatives:
			shortName, arch = name.rsplit('.', 1)
			if shortName == rpm:
				return name

		return None

	def alternativesWithoutScenario(self):
		if not self.requiresDisambiguation:
			return None

		result = set()
		for rpm in self.alternatives:
			if not rpm.newControllingScenarios:
				result.add(rpm)
		return result

	@property
	def closure(self):
		return self.alternatives.union(self.solutions)

class ConditionalDependency(object):
	def __init__(self, dep, parsed):
		self.dep = dep
		self.parsed = parsed

class PackageDependencies(object):
	unresolvableRpm = None

	def __init__(self, requiringPkg, key):
		self.key = key
		self.requiringPkg = requiringPkg
		self._resolved = []
		self.conditionals = []

		self.validScenarioChoices = []
		self.controllingScenarios = requiringPkg.newControllingScenarios
		self.isResolvable = True
		self.disfavoredRpms = set()
		self.failedAlternatives = None

		self.abiCompatibility = AbiManager.Compatibility(self.requiringPkg.name)

	def addResolved(self, dep, solution = None):
		rd = ResolvedDependency(dep, solution)
		self._resolved.append(rd)
		return rd

	def markUnresolvable(self, dep):
		assert(self.unresolvableRpm is not None)
		rd = self.addResolved(dep, self.unresolvableRpm)
		self.isResolvable = False
		return rd

	def addSolution(self, dep, solution):
		return self.addResolved(dep, solution)

	def addAmbiguousSolution(self, dep, rpms, acceptable = False):
		rd = self.addResolved(dep)
		rd.addAlternatives(rpms, acceptable)
		return rd

	def addConditional(self, dep, parsed):
		# strip out any white space; this will allow NewDB.load() to split the
		# line just using str.split()
		parsed = parsed.replace(' ', '')
		rd = ConditionalDependency(dep, parsed)
		self.conditionals.append(rd)
		return rd

	def __str__(self):
		return f"{self.requiringPkg}"

	def __iter__(self):
		return iter(self._resolved)

	@property
	def isAmbiguous(self):
		return any(rd.requiresDisambiguation for rd in self)

	@property
	def version(self):
		solvable = self.requiringPkg.solvable
		if solvable is None:
			return None

		evr = solvable.evr
		return evr.rsplit('-', maxsplit = 1)[0]

	def canAttemptScenarioDisambiguation(self, verbose = False):
		errors = 0
		for rd in self._resolved:
			lacking = rd.alternativesWithoutScenario()
			if lacking:
				if verbose:
					errormsg(f"{self}/{rd}: no scenario for {' '.join(sorted(map(str, lacking)))}")
				errors += 1

		return errors == 0

	def display(self):
		infomsg(f"resolved_{self.key}:{self.requiringPkg}")
		for rd in self._resolved:
			infomsg(f"  dep:{rd.dep}")
			for rpm in rd.solutions:
				infomsg(f"   - {rpm}")

			if rd.alternatives:
				for choice in rd.alternatives:
					infomsg(f"   - {choice}")

class PackageResolution(object):
	def __init__(self, requiringPkg, scenarioVersion = None):
		self.requiringPkg = requiringPkg
		self.scenarioVersion = scenarioVersion
		self.byDepId = {}

		self.validScenarioChoices = None
		self.effectiveDependencies = None
		self.controllingScenarios = requiringPkg.newControllingScenarios

	def __str__(self):
		if self.scenarioVersion is None:
			return str(self.requiringPkg)
		return f"{self.requiringPkg} for scenario {self.scenarioVersion}"

	def __iter__(self):
		return iter(self.byDepId.values())

	@property
	def version(self):
		solvable = self.requiringPkg.solvable
		if solvable is None:
			return None

		evr = solvable.evr
		return evr.rsplit('-', maxsplit = 1)[0]

	@property
	def isAmbiguous(self):
		return not all(rd.acceptableAmbiguity for rd in self)

	# Things can get a little complicated with depenedencies like:
	#  ((libjack0 and libjacknet0 and libjackserver0) or pipewire-jack)
	# libsolv will return three different solvables tied to three
	# different rules; one for each of libjack0, libjacknet0 and
	# libjackserver0, respectively.
	# It will not flag an alternative for pipewire-jack; for some odd
	# reason.
	def addResolved(self, dep, rpm):
		depId = dep.id

		rd = self.byDepId.get(depId)
		if rd is not None:
			rd.solutions.add(rpm)
		else:
			rd = ResolvedDependency(dep, solution = rpm)
			self.byDepId[depId] = rd

		return rd

	def getResolved(self, dep):
		return self.byDepId.get(dep.id)

	def display(self):
		for rd in self:
			infomsg(f"{self.requiringPkg} {rd.dep}")
			for rpm in rd.solutions:
				infomsg(f"   - {rpm}")

			if rd.alternatives:
				for choice in rd.alternatives:
					infomsg(f"   - {choice}")

	def displayAmbiguities(self):
		for rd in self:
			if rd.alternatives:
				infomsg(f"  {rd}")
				for alt in rd.alternatives:
					infomsg(f"     {alt}")

class ResolverLog(object):
	class Indent(object):
		def __init__(self, fp, ws = ""):
			self.fp = fp
			self.ws = ws

		def nest(self):
			return self.__class__(self.fp, self.ws + "   ")

		def print(self, msg):
			print(self.ws + msg, file = self.fp)

	def __init__(self, path):
		self.path = path
		self.fp = open(path, "w")

		infomsg(f"Logging resolved dependencies to {self.path}")
		self.top = self.Indent(self.fp)

	def logResolvedPackage(self, result):
		nest = self.beginResult(result)
		for rd in result:
			self.logDependency(nest, rd)

	def logUnresolvablePackage(self, result, problems):
		nest = self.beginResult(result, resolved = False)
		for rd in result:
			self.logDependency(nest, rd)

		if problems:
			self.logProblems(nest, problems)

	def beginResult(self, result, resolved = True):
		rpm = result.requiringPkg

		words = []
		if not resolved:
			words.append("unresolved:")

		words.append(rpm.buildArch)
		words.append(rpm.name)

		if resolved:
			valid = result.validScenarioChoices
			if valid:
				words.append(f"valid:{','.join(sorted(map(str,valid)))}")

		self.top.print(' '.join(words))
		return self.top.nest()

	def logDependency(self, nest, rd):
		nest.print(f"{rd.dep}")

		nest = nest.nest()
		if rd.solutions:
			for rpm in rd.solutions:
				nest.print(f"{rpm}")
		else:
			for rpm in rd.alternatives:
				nest.print(f"alt: {rpm}")

	def logProblems(self, origNest, problems):
		for problem in problems:
			nest = origNest
			nest.print(f"%problem:")

			for issue in problem:
				nest = nest.nest()
				nest.print(f"{issue}")

##################################################################
# This is used to deal with boolean dependencies.
# Most of the heavy lifting occurs in DependencyParser, but we
# need to resolve package names and "name >= 1.7" type of
# expressions.
class DependencyOracle(object):
	solvOPER = {
		'=':	solv.REL_EQ,
		'<':	solv.REL_LT,
		'>':	solv.REL_GT,
		'<=':	solv.REL_LT | solv.REL_EQ,
		'>=':	solv.REL_GT | solv.REL_EQ,
		'!=':	solv.REL_GT | solv.REL_LT,
	}

	def __init__(self, hints, pool):
		self.hints = hints
		self.pool = pool

	# check for 'magic' macro names that we should not try to expand,
	# such as:
	#	product-update()
	# for now, this is hard-coded but will receive a command in hints.conf
	def isMacroInvocation(self, name):
		i = name.find('(')
		if i < 0:
			return False
		# infomsg(f"inspecting invocation of {name}")
		funcName = name[:i]
		return funcName in ('product-update', )

	def whatprovides(self, name, flags = None, evr = None):
		nameId = self.pool.str2id(name)

		if flags is None:
			depId = nameId
		else:
			evrId = self.pool.str2id(evr)
			oper = self.solvOPER[flags]
			depId = self.pool.rel2id(nameId, evrId, oper)

		choices = set(self.pool.whatprovides(depId))
		return choices

class AbiManager(object):
	class ABI(object):
		def __init__(self, name, version):
			self.name = name
			self.version = version

		def __str__(self):
			return f"{self.name} = {self.version}"

	class Compatibility(object):
		def __init__(self, name):
			self.name = name
			self._map = {}

		def add(self, abi):
			if self.conflicts(abi):
				raise Exception(f"Bad abi usage in {self.name}: {abi} vs. {self._map[abi.name]}")
			self._map[abi.name] = abi

		def conflicts(self, abi):
			if abi is None:
				return None
			current = self._map.get(abi.name)
			if current is None or current is abi:
				return None
			return current

		def __str__(self):
			return ' '.join(sorted(map(str, self._map.values())))

	def __init__(self, abiKeys):
		self.keys = abiKeys
		self._all = {}
		self._abi = {}
		self._provider = {}

	@classmethod
	def splitDependency(klass, dep):
		dep = str(dep)
		if ' = ' not in dep:
			return None, None

		name, version = dep.split('=', maxsplit = 1)
		return name.strip(), version.strip()

	def dependencyToAbi(self, dep):
		name, version = self.splitDependency(dep)
		if name not in self.keys:
			return None

		key = f"{name}={version}"

		abi = self._all.get(key)
		if abi is None:
			abi = self.ABI(name, version)
			self._all[key] = abi
		return abi

	def addProvider(self, abi, solvable):
		self._abi[solvable.id] = abi

	def getAbi(self, solvable):
		return self._abi.get(solvable.id)

class PreprocessorHints(object):
	class AcceptableRpmSet(object):
		def __init__(self, nameList):
			self.nameList = nameList
			self.rpms = None

		def rebind(self, rpmFactory):
			self.rpms = set(filter(bool, map(rpmFactory.getByName, self.nameList)))

		def check(self, choices):
			return choices.issubset(self.rpms)

		def checkBuildNames(self, nameList):
			return False

	class AcceptableBuildSet(object):
		def __init__(self, nameList):
			self.nameList = nameList

		def rebind(self, rpmFactory):
			pass

		def check(self, choices):
			builds = set(rpm.buildName for rpm in choices)
			return builds.issubset(self.nameList)

		def checkBuildNames(self, nameList):
			return nameList.issubset(self.nameList)

	class HeuristicTransform(object):
		def __init__(self, name):
			self.name = name

		def __str__(self):
			return f"heuristic:{self.name}"

		def rebind(self, rpmFactory):
			pass

	class MiniRpmTransform(HeuristicTransform):
		def __call__(self, selection):
			drop = []
			for rpm in selection.rpms:
				name = rpm.shortname
				if '-mini' in name:
					regularName = name.replace('-mini', '')
					other = selection.nameToRpm(regularName)
					if other is not None:
						if rpm.trace or other.trace:
							infomsg(f"{self}: prefer {other} over {rpm}")
						drop.append(rpm)

			if drop:
				selection.difference_update(drop)

	class StripSuffixTransform(HeuristicTransform):
		def __call__(self, selection):
			drop = []
			for rpm in selection.rpms:
				name = rpm.shortname
				if name.endswith(self.SUFFIX):
					regularName = name[:-self.SUFFIX_LEN]
					other = selection.nameToRpm(regularName)
					if other is not None:
						if rpm.trace or other.trace:
							infomsg(f"{self}: prefer {other} over {rpm}")
						drop.append(rpm)

			if drop:
				selection.difference_update(drop)

	class StripBootstrapSuffixTransform(StripSuffixTransform):
		SUFFIX	= '-bootstrap'
		SUFFIX_LEN = len(SUFFIX)

	class StripDevelSuffixTransform(StripSuffixTransform):
		SUFFIX	= '-devel'
		SUFFIX_LEN = len(SUFFIX)

	class Strip32BitSuffixTransform(StripSuffixTransform):
		SUFFIX	= '-32bit'
		SUFFIX_LEN = len(SUFFIX)

	class AmbiguityTransform(object):
		def __init__(self, srcNameList, dstNameList):
			self.srcNameList = srcNameList
			self.dstNameList = dstNameList
			self.srcRpms = None
			self.dstRpms = None
			self.valid = False

		def rebind(self, rpmFactory):
			self.srcRpms = set(map(rpmFactory.getByName, self.srcNameList))
			self.dstRpms = set(map(rpmFactory.getByName, self.dstNameList))
			self.valid = (None not in self.srcRpms) and (None not in self.dstRpms)

		def __str__(self):
			return f"AmbiguityTransform([{' '.join(self.srcNameList)}] -> [{' '.join(self.dstNameList)}])"

		def __call__(self, selection):
			if self.valid and self.srcRpms.issubset(selection.rpms):
				selection.difference_update(self.srcRpms)
				selection.update(self.dstRpms)

	class StripPrefixTransform(HeuristicTransform):
		def __call__(self, selection):
			drop = []
			for rpm in selection.rpms:
				name = rpm.shortname
				if name.startswith(self.PREFIX):
					regularName = name[self.PREFIX_LEN:]
					other = selection.nameToRpm(regularName)
					if other is not None:
						if rpm.trace or other.trace:
							infomsg(f"{self}: prefer {other} over {rpm}")
						drop.append(rpm)

			if drop:
				selection.difference_update(drop)

	class StripBusyboxPrefixTransform(StripPrefixTransform):
		PREFIX	= 'busybox-'
		PREFIX_LEN = len(PREFIX)

	class AlwaysPreferTransform(object):
		def __init__(self, nameList):
			self.nameList = nameList
			self.rpms = None

		def rebind(self, rpmFactory):
			self.rpms = set(filter(bool, map(rpmFactory.getByName, self.nameList)))

		def __str__(self):
			return f"AlwaysPreferTransform()"

		def __call__(self, selection):
			common = self.rpms.intersection(selection.rpms)
			if common:
				selection.replace(common)

	class WildcardPreferBase(object):
		def __init__(self, preferredName, alternativeNames):
			self.preferredName = preferredName
			self.alternativeNames = alternativeNames
			self.dropped = None

		def begin(self, selection, preferredRpm):
			self.trace = any(rpm.trace for rpm in selection.rpms)
			self.preferredRpm = preferredRpm
			self.selection = selection
			self.dropped = set()

		def done(self):
			if self.dropped:
				if self.trace:
					infomsg(f"prefer {self.preferredRpm} over {' '.join(map(str, self.dropped))}")
				self.selection.difference_update(self.dropped)
			self.dropped = None

		def __str__(self):
			return f"{self.__class__.__name__}([{' '.join(self.alternativeNames)}] -> [{self.preferredName}])"

		def maybeDrop(self, regex, stripPrefix = None):
			for other in self.selection.rpms:
				if other is self.preferredRpm:
					continue

				name = other.shortname
				if stripPrefix is not None:
					if not name.startswith(stripPrefix):
						continue
					name = name[len(stripPrefix):]
				if regex.fullmatch(name):
					self.dropped.add(other)

	# prefer lib.* over \0-devel
	class WildcardPrefer1(WildcardPreferBase):
		def __init__(self, preferredName, alternativeNames):
			super().__init__(preferredName, alternativeNames)
			self.preferredPattern = re.compile(preferredName)
			if self.preferredPattern.groups == 0:
				self.preferredPattern = re.compile(f"({preferredName})")

			self.suffixREs = []
			self.alternativeExpansions = []
			for pattern in alternativeNames:
				if pattern.startswith('\\1') and pattern.count('\\') == 1:
					# at match time, we remove the matched pattern from other
					# rpms, then check the suffix against the RE
					self.suffixREs.append(re.compile(pattern[2:]))
				else:
					self.alternativeExpansions.append(pattern)

		def process(self, selection):
			for rpm in sorted(selection.rpms, key = lambda r: len(r.shortname)):
				if rpm not in selection.rpms:
					continue

				m = self.preferredPattern.fullmatch(rpm.shortname)
				if not m:
					continue

				self.begin(selection, rpm)
				for suffixRE in self.suffixREs:
					self.maybeDrop(suffixRE, stripPrefix = m.group(1))
				for s in self.alternativeExpansions:
					fullRE = re.compile(m.expand(s))
					self.maybeDrop(fullRE)
				self.done()

	# prefer perl over perl-[A-Z].*
	class WildcardPrefer2(WildcardPreferBase):
		def __init__(self, preferredName, alternativeNames):
			super().__init__(preferredName, alternativeNames)
			self.alternativePatterns = list(map(re.compile, alternativeNames))

		def process(self, selection):
			preferredRpm = selection.nameToRpm(self.preferredName)
			if preferredRpm is None:
				return

			self.begin(selection, preferredRpm)
			for re in self.alternativePatterns:
				self.maybeDrop(re)
			self.done()

	class WildcardPreferTransform(object):
		def __init__(self, select):
			self.select = select

		def rebind(self, rpmFactory):
			pass

		def __str__(self):
			return f"WildcardAmbiguityTransform({self.select})"

		def __call__(self, selection):
			if len(selection.rpms) > 1:
				self.select.process(selection)

		@classmethod
		def factory(klass, preferredNames, alternativeNames):
			if len(preferredNames) != 1:
				errormsg(f"wildcard prefer rules can have only one preferred package")
				return None

			preferredName, = preferredNames
			if any('\\1' in name for name in alternativeNames):
				# preferredName is a regexp, alternativeNames are expansion templates
				select = PreprocessorHints.WildcardPrefer1(preferredName, alternativeNames)
			else:
				# preferredName is a literal, alternativeNames are regexes
				select = PreprocessorHints.WildcardPrefer2(preferredName, alternativeNames)

			return klass(select)

	class PreTransform(object):
		def __init__(self, srcName, dstName, context = None):
			self.srcName = srcName
			self.dstName = dstName
			self.context = context

		def transform(self, fromName, requringRpmName):
			if self.context is not None and self.context != requringRpmName:
				return None
			return self.dstName

	class NoWarnRequired(object):
		def __init__(self):
			self.requiring = set()
			self.required = set()

		def filter(self, requiringName, requiredNames):
			if requiringName in self.requiring:
				return None
			return requiredNames.difference(self.required)

	def __init__(self):
		self.ignoreNames = []
		self.preferredNames = []
		self.syntheticNames = []
		self.knownMissingNames = []
		self.buildNoVersionCheckSet = set()
		self.acceptableAmbiguities = []
		self.ambiguityTransforms = []
		self.dependencyTransforms = {}
		self.acceptUnknownAmbiguities = False

		self._nameFilter = OBSNameFilter()

		self._newScenarioManager = NewScenarioManager()
		self._nowarnRequired = self.NoWarnRequired()
		self._alwaysPreferTransform = None

	def addKnownMissing(self, names):
		self.knownMissingNames += names

	def addIgnoredDependencies(self, names):
		self.ignoreNames += list(names)

	def addIgnoredRpm(self, pattern):
		self._nameFilter.addRpmPattern(pattern)

	def addIgnoredSuffixes(self, names):
		for name in names:
			self.addIgnoredRpm(f"*{name}")

	def addIgnoredRpms(self, names):
		for name in names:
			self.addIgnoredRpm(name)

	def addIgnoredBuilds(self, names):
		for pattern in names:
			self._nameFilter.addBuildPattern(pattern)

	def ignorePackageName(self, name):
		return self._nameFilter.matchRpm(name)

	def addIgnoredBuild(self, pattern):
		self._nameFilter.addBuildPattern(pattern)

	def ignoreBuildName(self, name):
		return self._nameFilter.matchBuild(name)

	def setAcceptUnknownAmbiguities(self, value):
		self.acceptUnknownAmbiguities = value

	# For each architecture, we start over and re-do the mapping of names to Rpm objects
	def rebind(self, rpmFactory):
		for obj in self.acceptableAmbiguities:
			obj.rebind(rpmFactory)
		for obj in self.ambiguityTransforms:
			obj.rebind(rpmFactory)
#		for abstractPackage in self.abstractScenarios:
#			abstractPackage.rebind(rpmFactory)

		allRpms = rpmFactory.getAllByType(RpmWrapper.TYPE_REGULAR)
		self._newScenarioManager.rebind(allRpms)

	def addPreferredNames(self, args):
		self.preferredNames += args
		if self._alwaysPreferTransform is None:
			self._alwaysPreferTransform = self.AlwaysPreferTransform(self.preferredNames)
			self.ambiguityTransforms.insert(0, self._alwaysPreferTransform)

	def addConditional(self, name, value):
		pass

	def addDependencyTransform(self, fromString, toString, **kwargs):
		xfrm = self.PreTransform(fromString, toString, **kwargs)
		self.dependencyTransforms[fromString] = xfrm

	def transformDependency(self, string, context):
		if ' ' in string:
			string = string.split()[0]

		xfrm = self.dependencyTransforms.get(string)
		if xfrm is None:
			return None

		return xfrm.transform(string, context)

	def addSyntheticNames(self, args):
		self.syntheticNames += args

	def defineAcceptableAmbiguity(self, nameList, type = 'rpm'):
		if type == 'rpm':
			self.acceptableAmbiguities.append(self.AcceptableRpmSet(nameList))
		elif type == 'build':
			self.acceptableAmbiguities.append(self.AcceptableBuildSet(nameList))
		else:
			errormsg(f"invalid object type {type}")
			return False

	def areAlternativesAcceptable(self, choices):
		# First check all accept-ambiguity rules defined in the hints file
		for ambig in self.acceptableAmbiguities:
			if ambig.check(choices):
				return 1

		# As a fallback, check for accept-unknown-ambiguities.
		# Note, this does not apply to dependency resolutions involving
		# scenarios (which we want to handle properly).
		if self.acceptUnknownAmbiguities and \
		   not any(rpm.newControllingScenarios for rpm in choices):
			return 2
		return 0

	def checkBuildAlternatives(self, builds):
		buildNames = set(build.name for build in builds)
		for rule in self.acceptableAmbiguities:
			if rule.checkBuildNames(buildNames):
				return 1
		return 0

	def skipVersionChecks(self, nameList):
		self.buildNoVersionCheckSet.update(nameList)

	def defineAmbiguityTransform(self, srcNames, dstNames):
		self.ambiguityTransforms.append(self.AmbiguityTransform(srcNames, dstNames))

	def enableHeuristics(self, *args):
		for name in args:
			if name == 'ignore-mini-packages':
				self.ambiguityTransforms.append(self.MiniRpmTransform(name))
			elif name == 'ignore-devel-alternative':
				self.ambiguityTransforms.append(self.StripDevelSuffixTransform(name))
			elif name == 'ignore-bootstrap-alternative':
				self.ambiguityTransforms.append(self.StripBootstrapSuffixTransform(name))
			elif name == 'ignore-32bit-alternative':
				self.ambiguityTransforms.append(self.Strip32BitSuffixTransform(name))
			elif name == 'ignore-busybox-alternative':
				self.ambiguityTransforms.append(self.StripBusyboxPrefixTransform(name))
			else:
				errormsg(f"Unknown heuristic {name}")
				return False

	# These two are exactly the same:
	#	prefer foo over bar
	# and
	#	transform-ambiguity foo bar into foo
	def definePreference(self, preferredNames, alternativeNames):
		self.defineAmbiguityTransform(preferredNames + alternativeNames, preferredNames)

	def defineWildcardPreference(self, preferredNames, alternativeNames):
		# wildcard rules can look like this:
		#   prefer python311-.* over \0[0-9_.]+
		#	this will disambiguate between different versions of the same python module
		#   prefer perl over perl-.*
		#	this will prefer perl over any perl-Foobar module
		transform = self.WildcardPreferTransform.factory(preferredNames, alternativeNames)
		self.ambiguityTransforms.append(transform)

	class RpmSelection(object):
		def __init__(self, rpms):
			self.rpms = set(rpms)
			self._names = None
			self._builds = None

		def __bool__(self):
			return bool(self.rpms)

		def __len__(self):
			return len(self.rpms)

		def __str__(self):
			return ' '.join(map(str, self.rpms))

		def discard(self, rpm):
			self.rpms.discard(rpm)
			if self._names is not None:
				self._names.pop(rpm.shortname, None)
			self._builds = None

		@property
		def nameDict(self):
			if self._names is None:
				self._names = dict((rpm.shortname, rpm) for rpm in self.rpms)
			return self._names

		@property
		def names(self):
			return set(self.nameDict.keys())

		def nameToRpm(self, name):
			return self.nameDict.get(name)

		@property
		def builds(self):
			if self._builds is None:
				self._builds = set(rpm.buildName for rpm in self.rpms)
			return self._builds

		def difference_update(self, rpms):
			self.rpms.difference_update(rpms)
			self._names = None
			self._builds = None

		def update(self, rpms):
			self.rpms.update(rpms)
			self._names = None
			self._builds = None

		def replace(self, rpms):
			self.rpms = rpms
			self._names = None
			self._builds = None

	def filterChoices(self, choices):
		selection = self.RpmSelection(choices)

		debugSolver(f"filterChoices {selection}")
		namesToDrop = selection.names.intersection(self.ignoreNames)
		if namesToDrop:
			rpmsToDrop = set(rpm for rpm in selection.rpms if rpm.shortname in namesToDrop)
			selection.difference_update(rpmsToDrop)

		if not selection:
			errormsg(f"All names in selection ignored - not good")
			return None

		for transform in self.ambiguityTransforms:
			transform(selection)
			if not selection:
				raise Exception(f"transform {transform} reduces selection to empty set")

		return selection.rpms

	def hasScenarioVariable(self, *args, **kwargs):
		return self._newScenarioManager.hasVariable(*args, **kwargs)

	def getScenarioVariableValues(self, *args, **kwargs):
		return self._newScenarioManager.getPredefinedVariablesValues(*args, **kwargs)

	def createScenarioVariable(self, name, *values, **kwargs):
		self._newScenarioManager.createVariable(name, values, **kwargs)

	def defineVariableFallback(self, name, key, values):
		var = self._newScenarioManager.getScenarioVariable(name)
		if key not in var.values:
			errormsg(f"cannot define fallback for {name}/{key}: unknown value for this variable")
			return False
		var.setFallback(key, values)

	# For a scenario like "jdk" add a group of equivalent rpms, e.g. "java-headless".
	# The objective is to detect packages that depend on "any headless jdk" and
	# resolve this ambiguity by replacing it with a synthetic rpm named
	# "jdk/java-headless"
	def defineConcreteScenario(self, scenarioSpec, key, rpmNames):
		try:
			scenarioName, abstractPackageName = scenarioSpec.split('/')
		except:
			errormsg(f"expected \"scenario/key\" argument")
			return False

		if not self.hasScenarioVariable(scenarioName):
			errormsg(f"unknown scenario \"{scenarioName}\"")
			return False

		var = self._newScenarioManager.getScenarioVariable(scenarioName)

		if key != '%':
			# Simple case: the scenario definition defines an rpm list for a specific version
			concreteScenario = self._newScenarioManager.createConcreteScenario(scenarioName, key, abstractPackageName)
			self._newScenarioManager.mapConcreteScenario(concreteScenario, rpmNames)
			return

		for name in rpmNames:
			pattern = name.replace('%', '([0-9._]+|[a-z_]*)') + '$'
			self._newScenarioManager.addConcreteScenarioPattern(scenarioName, abstractPackageName, pattern)

		# for each known variable value, instantiate the corresponding name->scenario link
		# We need to do this because the wildcard pattern is limited in what it matches
		for value in self._newScenarioManager.getPredefinedVariablesValues(scenarioName):
			concreteScenario = self._newScenarioManager.createConcreteScenario(scenarioName, value, abstractPackageName)
			mappedNames = list(name.replace('%', value) for name in rpmNames)
			self._newScenarioManager.mapConcreteScenario(concreteScenario, mappedNames)

	def checkForUnhandledScenarios(self, rpms, **kwargs):
		return self._scenarioManager.checkForUnhandledScenarios(rpms, **kwargs)

	def suppressWarnings(self, kind, *args):
		if kind == 'requiring':
			self._nowarnRequired.requiring.update(args)
		elif kind == 'required':
			self._nowarnRequired.required.update(args)
		else:
			return False
		return True

	def filterUnresolvedRequirements(self, requiringName, requiredNames):
		return self._nowarnRequired.filter(requiringName, requiredNames)

class PreprocessorHintsLoader(object):
	def __init__(self, filename):
		self.filename = filename
		self.current = None
		self.lineno = 0
		self.error_line = None
		self.errors = 0

	def load(self):
		self.hints = PreprocessorHints()

		infomsg(f"Loading preprocessor hints from {self.filename}")
		with open(self.filename) as f:
			self.currentContext = None
			for line in f.readlines():
				self.lineno += 1

				if '#' in line:
					line = line[:line.index('#')]
				line = line.rstrip()
				if not line:
					continue

				if line[0].isspace():
					# This is a continuation
					self.processContinuation(line)
				else:
					self.beginMultilineCommand(line)

		self.flushMultilineCommand()

		if self.errors:
			raise Exception(f"Encountered {self.errors} errors while parsing {self.filename}")

		return self.hints

	def validate(self, hints):
		hints._scenarioManager.validate()

	def expandDefaultRules(self, hints):
		hints._scenarioManager.expandDefaultRules()

	def error(self, msg):
		lineno = self.error_line
		if lineno is None:
			lineno = self.lineno

		infomsg(f"{self.filename}:{lineno}: {msg}")
		self.errors += 1
		return False

	def flushMultilineCommand(self):
		if self.current is not None:
			self.processMultilineCommand(self.current)
		self.error_line = None

	def beginMultilineCommand(self, line):
		self.flushMultilineCommand()

		self.error_line = self.lineno
		self.current = self.GenericMultilineCommand(line.split())

	def processMultilineCommand(self, cmd):
		words = cmd.words
		if len(words) >= 5 and words[0] == 'with':
			if words[1] != 'scenario':
				errormsg(f"{self}: 'with' keyword must be followed by 'scenario'")
				return False

			name = words[2]

			variableValues = self.hints.getScenarioVariableValues(name)
			if not variableValues:
				errormsg(f"{self}: unknown scenario {name}")
				return False

			for version in variableValues:
				versionWords = []
				for s in words[3:]:
					versionWords.append(s.replace('%', version))
				if not self.processCommandWords(versionWords):
					return False
		else:
			return self.processCommandWords(words)

	class Command(object):
		def __init__(self, name, minArgs, keywords = [], call = None, types = None):
			self.name = name
			if type(minArgs) is int:
				maxArgs = None
			else:
				minArgs, maxArgs = minArgs
			self.minArgs = minArgs
			self.maxArgs = maxArgs
			self.keywords = keywords
			self.types = types
			self.context = None
			self.call = call

		def __str__(self):
			return self.name

		def __call__(self, *args, **kwargs):
			return self.call(*args, **kwargs)

		def convertArgument(self, words, pos, targetType):
			value = words[pos]
			if targetType == 'bool':
				if value in ('true', '1'):
					words[pos] = True
					return True
				if value in ('false', '0'):
					words[pos] = False
					return True
			return False

	class StarCommand(Command):
		def __call__(self, hints, words, **kwargs):
			return super().__call__(hints, *words, **kwargs)

	class VariableCommand(Command):
		def __call__(self, hints, words, **kwargs):
			name = words.pop(0)
			return super().__call__(hints, name, words, **kwargs)

	class InfixCommand(Command):
		def __init__(self, *args, splitWord = None, **kwargs):
			super().__init__(*args, **kwargs)
			assert(splitWord is not None)
			self.splitWord = splitWord

		def __call__(self, hints, words, **kwargs):
			if self.splitWord not in words:
				errormsg(f"{self}: lacking {self.splitWord} keyword")
				return False

			i = words.index(self.splitWord)
			if i == 0 or i == len(words) - 1:
				errormsg(f"{self}: {self.splitWord} keyword must not be the first or last argument")
				return False

			fromValues = words[:i]
			toValues = words[i+1:]
			return super().__call__(hints, fromValues, toValues, **kwargs)

	# Commands with this sort of syntax:
	#   cmd arg1 arg2 ...
	#	keyA: valueA1 valueA2
	#	keyB: valueB1 valueB2 valueB3
	#	...
	class MappingCommand(Command):
		def __call__(self, hints, words, **kwargs):
			args = []
			while words and not words[0].endswith(':'):
				args.append(words.pop(0))

			while words:
				key = words.pop(0)
				if not key.endswith(':'):
					return False

				key = key.rstrip(':')

				values = []
				while words and not words[0].endswith(':'):
					values.append(words.pop(0))

				if super().__call__(hints, *args, key, values) is False:
					return False

	class CommandGroup(object):
		def __init__(self, name):
			self.name = name
			self.setter = None
			self.valid = False

	COMMAND_LIST = [
	Command('ignore',			1,	call = PreprocessorHints.addIgnoredDependencies),
	Command('ignore-suffix',		1,	call = PreprocessorHints.addIgnoredSuffixes),
	Command('ignore-rpm',			1,	call = PreprocessorHints.addIgnoredRpms),
	Command('ignore-build',			1,	call = PreprocessorHints.addIgnoredBuilds),
	Command('synthetic',			1,	call = PreprocessorHints.addSyntheticNames),
	Command('always-prefer',		1,	call = PreprocessorHints.addPreferredNames),
	Command('accept-missing',		1,	call = PreprocessorHints.addKnownMissing),
	Command('accept-ambiguity',		1,	call = PreprocessorHints.defineAcceptableAmbiguity,
							keywords = ('type', )),
	Command('build-skip-version-check',
						1,	call = PreprocessorHints.skipVersionChecks),
	MappingCommand('scenario',		3,	call = PreprocessorHints.defineConcreteScenario,
							keywords = None),
	StarCommand('variable',			1,	call = PreprocessorHints.createScenarioVariable,
							keywords = ('pattern', )),
	MappingCommand('fallback',		2,	call = PreprocessorHints.defineVariableFallback),
	StarCommand('pre-transform',		[2, 2],	call = PreprocessorHints.addDependencyTransform,
							keywords = 'context'),
	StarCommand('conditional',		[2, 2],	call = PreprocessorHints.addConditional,
							types = [None, 'bool']),
	StarCommand('accept-unknown-ambiguities',
						[1, 1],	call = PreprocessorHints.setAcceptUnknownAmbiguities,
							types = ['bool']),
	StarCommand('nowarn',			1,	call = PreprocessorHints.suppressWarnings),
	StarCommand('heuristic',		1,	call = PreprocessorHints.enableHeuristics),
	InfixCommand('transform-ambiguity',	2,	call = PreprocessorHints.defineAmbiguityTransform,
							splitWord = 'into'),
	InfixCommand('prefer',			2,	call = PreprocessorHints.definePreference,
							splitWord = 'over'),
	InfixCommand('match-prefer',		2,	call = PreprocessorHints.defineWildcardPreference,
							splitWord = 'over'),
	]
	COMMANDS = {}

	def handleCommand(self, cmd, words):
		args = []
		kwargs = {}
		for w in words:
			if '=' not in w or cmd.keywords is None:
				args.append(w)
			else:
				key, value = w.split('=')
				if key not in cmd.keywords:
					return self.error(f"{cmd} does not accept argument {w}")
				kwargs[key] = value

		if cmd.minArgs == cmd.maxArgs:
			if len(args) != cmd.minArgs:
				return self.error(f"{cmd} expects exactly {cmd.minArgs} argument(s) ({len(args)} were given)")
		else:
			if len(args) < cmd.minArgs:
				return self.error(f"not enough arguments for {cmd}")
			if cmd.maxArgs is not None and len(args) > cmd.maxArgs:
				return self.error(f"too many arguments for {cmd}")

		if cmd.types is not None:
			for i in range(len(cmd.types)):
				targetType = cmd.types[i]
				if targetType is None:
					pass
				elif not cmd.convertArgument(args, i, targetType):
					return self.error(f"{cmd}: invalid type for argument #{i}: not a valid {targetType}")

		if self.currentContext is not cmd.context and \
		   self.currentContext is not None:
			# The next command has not context, or a different one
			self.currentContext.valid = False

		self.currentContext = cmd.context
		if self.currentContext is not None:
			# reset the context
			if self.currentContext.setter is cmd:
				self.currentContext.valid = False
			elif not self.currentContext.valid:
				return self.error(f"{cmd.name}: outside of context (should be preceded by {self.currentContext.setter})")

			kwargs['context'] = self.currentContext

		try:
			if cmd(self.hints, args, **kwargs) is False:
				return self.error(f"{cmd.name}: invalid argument(s): {' '.join(args)}")
		except Exception as e:
			return self.error(f"{cmd.name} {' '.join(args)}: caught exception {e}")

		if self.currentContext is not None and \
		   not self.currentContext.valid:
			return self.error(f"{cmd.name} was expected to initialize the context but did not")

		return True

	def initCommands(self):
		if self.COMMANDS:
			return

		self.COMMANDS = {}
		sharedContext = None
		for cmd in self.COMMAND_LIST:
			if isinstance(cmd, self.CommandGroup):
				sharedContext = cmd
				continue

			self.COMMANDS[cmd.name] = cmd
			cmd.context = sharedContext

			# First command in a group must be the one to initialize the context
			if sharedContext and sharedContext.setter is None:
				sharedContext.setter = cmd

	def processCommandWords(self, words):
		command = words.pop(0)

		self.initCommands()

		handler = self.COMMANDS.get(command)
		if handler is not None:
			return self.handleCommand(handler, words)

		return self.error(f"unsupported command {command}")

	def processContinuation(self, line):
		if self.current is None:
			return self.error(f"spurious line continuation")

		if not self.current.handleContinuation(line.strip()):
			self.error(f"bad arguments")

	class GenericMultilineCommand(object):
		def __init__(self, initialValues = []):
			self.words = [] + initialValues

		def __str__(self):
			words = self.words[:2]
			return f"{' '.join(words)} ..."

		def handleContinuation(self, line):
			if line.startswith("-"):
				self.words.append(line[1:].strip())
			else:
				self.words += line.split()
			return True
