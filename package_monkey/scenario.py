##################################################################
#
# Scenarios describe a structured set of alternatives.
# This could be different versions of a package (jdk 1.8, 11, 17, ...),
# different implementations of a stack (docker vs podman), or
# other concepts like the product family.
#
# For example, each version of postgresql comes with a bunch of
# subpackages (server, client, devel, various language bindings).
# Sometimes, it happens that some other RPM depends on more than one
# postgres package, so you get one dependency that expands to say
# postgres{11,15,17}-devel and another that expands to
# postgres{11,15,17}-client. We want to be able to identify that
# this package can be used with 3 postgres scenarios (ie postgres version 11,
# postgres version 15 etc) and, whatever version, it always depends on
# the corresponding postgres devel and postgres client packages.
#
# This is why we introduce abstract scenarios like "postgres/client"
# and "postgres/devel".
#
# Overview of terms used below:
#
# Scenario variable: 
#	Example: jdk, which takes values 1.8, 11, ...
#
# Abstract package:
#	A meta-package like "the openjdk headless package" hiding
#	the multiple versions that actually exist.
#
#	Example: jdk/headless, which expands to different package
#	names depending on the jdk version.
#	 jdk=11:	java-11-openjdk-headless
#	 jdk=17:	java-17-openjdk-headless
#	 ... etc
#
# Concrete package:
#	Abstract package, expressed for a specific value.
#	Example: jdk=11/headless, with corresponding rpm
#	java-11-openjdk-headless
##################################################################

from .util import debugmsg, infomsg, warnmsg, errormsg, loggingFacade
from .util import OptionalCaption
from .pmatch import ParallelStringMatcher
import re
import functools

__names__ = ['NewScenarioManager', 'ScenarioSalad', 'ScenarioTupleSet', 'ScenarioTuple']

class ScenarioTuple(object):
	def __init__(self, variable, value, abstractPackage = None):
		self.variable = variable
		self.value = value
		self.abstractPackage = abstractPackage

	def __str__(self):
		s = f"{self.variable}/{self.value}"
		if self.abstractPackage:
			s += f"/{self.abstractPackage}"
		return s

	def __eq__(self, other):
		if self.variable != other.variable:
			return False
		if self.value != other.value:
			return False
		if self.abstractPackage != other.abstractPackage:
			return False
		return True

	def __hash__(self):
		return hash(str(self))

	@property
	def version(self):
		return NewScenarioVersion(self.variable, self.value)

	@property
	def symbolicRpmName(self):
		return f"{self.variable}/{self.abstractPackage}"

	@property
	def isComplete(self):
		return self.variable and self.value and self.abstractPackage

	# other can be a ScenarioTuple or a NewScenarioVersion
	def conflicts(self, other):
		return self.variable == other.variable and self.value != other.value

	@classmethod
	def parse(klass, s):
		w = s.split('/')
		return klass(*w)

class ScenarioVersionSingleton(object):
	_table = {}

	@classmethod
	def create(klass, variable, value):
		key = f"{variable}={value}"
		version = klass._table.get(key)
		if version is None:
			version = klass(variable, value)
			klass._table[key] = version
		return version

	def __init__(self, variable, value):
		self.variable = variable
		self.value = value

	def __str__(self):
		return f"{self.variable}={self.value}"

	# other can be a ScenarioTuple or a NewScenarioVersion
	def conflicts(self, other):
		return self.variable == other.variable and self.value != other.value

def NewScenarioVersion(*args):
	return ScenarioVersionSingleton.create(*args)

class ScenarioTupleSet(set):
	def __str__(self):
		return "{" + ', '.join(map(str, self)) + "}"

	@property
	def variables(self):
		return set(sct.variable for sct in self)

	@property
	def versions(self):
		return set(sct.version for sct in self)

	def variableVersions(self, variable):
		ret = set()
		for sct in self:
			if sct.variable == variable:
				ret.add(sct.version)
		return ret

	@property
	def packages(self):
		return set(sct.abstractPackage for sct in self)

	def copy(self):
		return self.__class__(self)

class ConcreteScenario(object):
	def __init__(self, sct):
		self.control = sct
		self.rpms = set()

	def __str__(self):
		return str(self.control)

	def addRpm(self, rpm):
		self.rpms.add(rpm)

class ConcreteScenarioSet(set):
	def __str__(self):
		return "{" + ', '.join(map(str, self)) + "}"

	def conflicts(self, version):
		return any(concreteScenario.control.conflicts(version) for concreteScenario in self)

class ScenarioVariable(object):
	def __init__(self, name, values):
		self.name = name
		self.values = list(values)
		self.pattern = None
		self.fallbacks = {}

	def setFallback(self, key, fallbacks):
		assert(key in self.values)
		self.fallbacks[key] = fallbacks

	def getFallbacks(self, key):
		return self.fallbacks.get(key, [])

class NewScenarioManager(object):
	def __init__(self):
		self._byId = {}
		self._byRpm = {}

		self._patterns = []

		self._variables = {}

	def createVariable(self, name, values):
		var = ScenarioVariable(name, values)
		self._variables[name] = var

	def hasVariable(self, name):
		return name in self._variables

	def getScenarioVariable(self, name):
		return self._variables.get(name)

	def getPredefinedVariablesValues(self, name):
		var = self._variables.get(name)
		if var is None:
			return []
		return var.values

	def createConcreteScenario(self, variable, value, abstractPackage):
		sct = ScenarioTuple(variable, value, abstractPackage)

		key = str(sct)
		concreteScenario = self._byId.get(key)
		if concreteScenario is None:
			concreteScenario = ConcreteScenario(sct)
			self._byId[key] = concreteScenario

		return concreteScenario

	def mapConcreteScenario(self, concreteScenario, rpmNames):
		assert(concreteScenario.control.value != '%')

		for rpm in rpmNames:
			if rpm not in self._byRpm:
				self._byRpm[rpm] = set()
			self._byRpm[rpm].add(concreteScenario)

	def addConcreteScenarioPattern(self, variable, abstractPackage, pattern):
		regex = re.compile(pattern)
		self._patterns.append((regex, variable, abstractPackage))

	def rebind(self, rpms):
		for concreteScenario in self._byId.values():
			concreteScenario.rpms = set()

		for rpm in rpms:
			name = rpm.shortname

			for concreteScenario in self._byRpm.get(name) or []:
				self.attachRpm(rpm, concreteScenario)

			for concreteScenario in self.matchPattern(name):
				self.attachRpm(rpm, concreteScenario)

		self.applyScenarioFallbacks()

	def matchPattern(self, name):
		for regex, variable, abstractPackage in self._patterns:
			m = regex.fullmatch(name)
			if not m:
				continue

			version = m.group(1)
			if not version:
				continue

			concreteScenario = self.createConcreteScenario(variable, version, abstractPackage)
			yield concreteScenario

	def applyScenarioFallbacks(self):
		allScenarios = list(self._byId.values())

		for var in self._variables.values():
			# don't bother unless this variable defines fallbacks
			if not var.fallbacks:
				continue

			abstractPackageNames = set()
			for scenario in allScenarios:
				if scenario.control.variable == var.name and scenario.rpms:
					abstractPackageNames.add(scenario.control.abstractPackage)

			for abstractPackage in abstractPackageNames:
				defined = {}
				undefined = []
				for version in var.values:
					concreteScenario = self.createConcreteScenario(var.name, version, abstractPackage)
					if concreteScenario.rpms:
						defined[version] = concreteScenario
					else:
						undefined.append(concreteScenario)

				for concreteScenario in undefined:
					targetVersion = concreteScenario.control.value
					fallbacks = var.getFallbacks(targetVersion).copy()

					while fallbacks:
						version = fallbacks.pop(0)

						found = defined.get(version)
						if found is None:
							fallbacks += var.getFallbacks(version)
							continue

						if False:
							infomsg(f"  {concreteScenario}: fall back to {found}")
						defined[targetVersion] = found
						for rpm in found.rpms:
							self.attachRpm(rpm, concreteScenario)
						break

	def attachRpm(self, rpm, concreteScenario):
		sct = concreteScenario.control
		if sct.value == '__auto__':
			version = rpm.extractVersion()
			concreteScenario = self.createConcreteScenario(sct.variable, version, sct.abstractPackage)
			
		if rpm.trace:
			infomsg(f"   {rpm} is part of {concreteScenario}")
		concreteScenario.addRpm(rpm)
		rpm.addControllingScenarioNew(concreteScenario)

	def lookupByRpm(self, rpm):
		return self._byRpm.get(rpm)

	def lookupByScenario(self, sct):
		return self._byId.get(str(sct))

class ScenarioSalad(object):
	class Bucket(object):
		def __init__(self, key):
			self.key = key
			self.availableScenarios = ConcreteScenarioSet()
			self.alternatives = set()

		def add(self, rpm, concreteScenario):
			self.alternatives.add(rpm)
			self.availableScenarios.update(rpm.newControllingScenarios)

	class DependencyRewrite(dict):
		def add(self, key, rpmName):
			self[key].add(rpmName)

	class Solution(object):
		def __init__(self, selectedVersions, symbolicRpmMap = None):
			self.selectedVersions = selectedVersions
			self.selectedScenarios = ConcreteScenarioSet()
			self.selectedRpms = set()

			self._symbolicRpms = symbolicRpmMap

		def __str__(self):
			return "|".join(sorted(map(str, self.selectedScenarios)))

		def updateWithBucket(self, b, selectedScenarios):
			self.selectedScenarios.update(selectedScenarios)
			for concreteScenario in selectedScenarios:
				self.selectedVersions.add(concreteScenario.control.version)

			selectedRpms = functools.reduce(set.union, (concreteScenario.rpms for concreteScenario in selectedScenarios))
			selectedRpms = selectedRpms.intersection(b.alternatives)
			self.selectedRpms.update(selectedRpms)

	class SolutionSet(object):
		def __init__(self, id, symbolicRpmMap):
			self.id = id
			self._solutions = []
			self._symbolicRpms = symbolicRpmMap

		def add(self, solution):
			self._solutions.append(solution)

		def __iter__(self):
			return iter(self._solutions)

		def __len__(self):
			return len(self._solutions)

		def __str__(self):
			return f"{' '.join(map(str, self._solutions))}"

		def getSymbolicRpms(self, key):
			return self._symbolicRpms[key]

		def createEmptySubset(self):
			return self.__class__(self.id, self._symbolicRpms)

		@property
		def commonVersion(self):
			versions = set()
			for solution in self._solutions:
				versions.update(solution.selectedVersions)
			if len(versions) != 1:
				return None
			return next(iter(versions))

	def __init__(self, id, controllingScenarios = None, trace = False):
		self.id = id
		self.controllingScenarios = controllingScenarios
		self.trace = trace
		self._buckets = []
		self._symbolicRpms = self.DependencyRewrite()

	def __str__(self):
		return self.id

	# the "key" corresponds to one dependency, and the
	# concreteScenario to one alternative solution offered by the solver.
	def add(self, key, alternatives):
		if not alternatives:
			errormsg(f"{self}: refusing to add {key} with empty list of alternatives")
			return True

		b = self.Bucket(key)
		self._buckets.append(b)

		conflicts = []

		# add all alternatives, except for those that conflict with our controlling scenario
		# For example, libc++1 requires libc++abi1. If libc++1 is from llvm21, it is controlled
		# by scenario llvm=21.
		# In terms of resolving dependencies, any llvmXX-libc++abi1 package can satisfy the
		# dependency, but for the same of simplicity, we just want libc++1 to require the
		# libc++abi1 rpm from the same version.
		for rpm in alternatives:
			for concreteScenario in rpm.newControllingScenarios:
				assert(rpm in concreteScenario.rpms)

				if self.controllingScenarios and \
				   self.controllingScenarios.conflicts(concreteScenario.control):
					problem = f"ignore {rpm} with scenario {concreteScenario.control}; conflict with {' '.join(map(str, self.controllingScenarios))}"
					conflicts.append(problem)
					if self.trace:
						infomsg(f"{self}: {problem}")
					continue

				b.add(rpm, concreteScenario)

		if not b.alternatives:
			errormsg(f"{self}: unable to resolve {key}: none of the alternatives is valid")
			for problem in conflicts:
				errormsg(f"   {problem}")
			return False

		self._symbolicRpms[key] = set()
		return True

	def solveBucket(self, b, versionSet):
		selectedScenarios = ConcreteScenarioSet()
		conflictingScenarios = ConcreteScenarioSet()

		for version in versionSet:
			for concreteScenario in b.availableScenarios:
				sct = concreteScenario.control
				if sct.variable != version.variable:
					# controlled by a different variable
					continue

				if sct.value == version.value:
					selectedScenarios.add(concreteScenario)
				else:
					conflictingScenarios.add(concreteScenario)

		if not selectedScenarios:
			if conflictingScenarios:
				return None
			return b

		# Sometimes we deal with multiple concrete scenarios that cover the same
		# RPM(s). One example is pipewire, where some dependencies resolve to
		# several libjack packages, where we currently have libjack0 covered
		# by jack/direct/libjack0, and by jack/direct/jack-server (which contains
		# jack libjack0 libjackserver0 libjacknet0).
		# Try to reduce that to a single scenario (which is the smallest one).
		if len(selectedScenarios) > 1:
			if self.trace:
				infomsg(f"{self} dependency {b.key}: ambiguous scenarios {' '.join(map(str, selectedScenarios))}")
				for other in selectedScenarios:
					infomsg(f"   {other}: {' '.join(map(str, other.rpms))}")

			smallest = sorted(selectedScenarios, key = lambda s: (len(s.rpms), str(s)))[0]
			if all(smallest.rpms.issubset(scenario.rpms) for scenario in selectedScenarios):
				if self.trace:
					infomsg(f"reduced to {smallest}")
				selectedScenarios = set((smallest, ))
			else:
				warnmsg(f"{self}: ambiguous set of scenarios: {' '.join(map(str, selectedScenarios))}")

		return selectedScenarios

	def enumerateScenariosForRefinement(self):
		available = ScenarioTupleSet()
		for b in self._buckets:
			for concreteScenario in b.availableScenarios:
				available.add(concreteScenario.control)

		if not available:
			return []

		# For each variable name, compute the set of (variable, version) pairs.
		# versionSets is a list of these sets
		versionSets = list(map(available.variableVersions, available.variables))

		# Sort by descending size
		versionSets.sort(key = lambda s: -len(s))

		return versionSets

	def permute(self, versionSetList):
		crossProduct = [[]]
		for versionSet in reversed(versionSetList):
			nextCrossProduct = []
			for version in versionSet:
				for prod in crossProduct:
					nextCrossProduct.append([version] + prod)
			crossProduct = nextCrossProduct
		return crossProduct

	def solveOnePermutation(self, versionSet):
		refined = []
		for b in self._buckets:
			selectedScenarios = self.solveBucket(b, versionSet)
			if selectedScenarios is None:
				return None

			refined.append((b, selectedScenarios))

		solution = self.Solution(versionSet, symbolicRpmMap = self._symbolicRpms)
		for b, selectedScenarios in refined:
			solution.updateWithBucket(b, selectedScenarios)

			for concreteScenario in selectedScenarios:
				self._symbolicRpms.add(b.key, concreteScenario.control.symbolicRpmName)

		return solution

	def solveAllPermutations(self, versionSetList):
		result = self.SolutionSet(self.id, self._symbolicRpms)
		with loggingFacade.temporaryIndent():
			for p in self.permute(versionSetList):
				if self.trace:
					infomsg(f"Try to solve using {' '.join(map(str, p))}")

				versionSet = ScenarioTupleSet(p)
				solution = self.solveOnePermutation(versionSet)
				if solution is None:
					if self.trace:
						infomsg(f" => cannot solve")
					continue

				if self.trace:
					infomsg(f" => solution: {' '.join(map(str, solution.selectedRpms))} with {' '.join(map(str, solution.selectedScenarios))}")

				result.add(solution)
		return result

	def solve(self):
		versionSetList = self.enumerateScenariosForRefinement()

		if self.trace:
			if self.controllingScenarios:
				infomsg(f"solve {self} (controlled by {' '.join(map(str, self.controllingScenarios))}):")
			else:
				infomsg(f"solve {self}:")

			infomsg(f"   found {len(versionSetList)} variables")
			for i in range(len(versionSetList)):
				vset = versionSetList[i]
				infomsg(f"      [{i}] versions: {', '.join(map(str, vset))}")

		solutions = self.solveAllPermutations(versionSetList)
		self.validateSolutions(solutions)
		return solutions

	def validateSolutions(self, solutions):
		if not solutions:
			raise Exception(f"{self}: no solutions found")

		allAlternatives = set()
		for b in self._buckets:
			allAlternatives.update(b.alternatives)

		allSolved = set()
		for sol in solutions:
			allSolved.update(sol.selectedRpms)

		if allSolved != allAlternatives:
			# This can happen, eg when postgres-foo has a depdency that resolves to
			# all postgresNN-foo packages
			extra = allSolved.difference(allAlternatives)
			if extra:
				raise Exception(f"{self}: spurious rpms added {' '.join(map(str, extra))}")

			lost = allAlternatives.difference(allSolved)
			if self.trace:
				infomsg(f"{self}: could not generate solutions covering {' '.join(map(str, lost))}")

		abstractPackages = set()
		for sol in solutions:
			for concreteScenario in sol.selectedScenarios:
				abstractPackages.add(concreteScenario.control.symbolicRpmName)

		if self.trace:
			infomsg(f"solution packages {' '.join(abstractPackages)}")
			for sol in solutions:
				infomsg(f" - {sol} packages {' '.join(map(str, sol._symbolicRpms.values()))}")
