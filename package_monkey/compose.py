##################################################################
#
# Product composition
#
##################################################################

from .reports import GenericStringReport
from .util import loggingFacade, infomsg, warnmsg, errormsg
from .util import locale_sorted
from .util import DictOfSets
from .csvio import CSVWriter
from .filter import Classification
from .sick_yaml import *
from .newdb import GenericRpm
from .arch import *
from .packages import PackageCollection, RpmOverrideList
from .scenario import *
from .rpmdeps import *

##################################################################
# The actual product composition
##################################################################
class ProductComposition(object):
	TYPE_EXTENSION		= 'extension'
	TYPE_BASEPRODUCT	= 'baseproduct'

	def __init__(self, id, classificationScheme):
		self.id = id
		self.name = id
		self.type = self.TYPE_BASEPRODUCT
		self.classificationScheme = classificationScheme
		self.architectures = []
		self.contractNames = []

		self.baseProductName = None
		self.baseProduct = None

		self.topics = None
		self.epics = None

		# used by the OBS productcomposer
		self.obsComposeKey = None
		self.releasePackage = None
		self.releaseEpic = None
		self.releaseRpms = PackageCollection()
		self.releaseScenario = None
		self.reasoning = None

		self.rpms = PackageCollection()

		# We can't juse use sets here, because the config file may contain entries
		# like 'foobar: [arch1, arch2]' ie a dict. You cannot have a set containing
		# dict elements (no __hash__ method).
		self._overrideRpmsInclude = RpmOverrideList()
		self._overrideRpmsExclude = RpmOverrideList()

	def __str__(self):
		return self.id

	@property
	def regularRpms(self):
		for rpm, archSet in self.rpms.rpmsWithArch():
			if not rpm.isSynthetic:
				yield rpm

	@property
	def regularRpmsWithArch(self):
		for rpm, archSet in self.rpms.rpmsWithArch():
			if not rpm.isSynthetic:
				yield rpm, archSet

	@property
	def allAvailableRpms(self):
		result = self.rpms.union(self.releaseRpms)
		if self.baseProduct is not None:
			result.update(self.baseProduct.allAvailableRpms)
		return result

	def getRpmArchitectures(self, rpm):
		productArchSet = ArchSet(self.architectures)

		archSet = self.rpms.getArch(rpm.name)
		if archSet is None:
			return productArchSet

		return productArchSet.intersection(archSet)

	def sortkey(self):
		if self.baseProduct:
			return f"{self.baseProduct.sortkey()}/{self.id}"
		return self.id

	def updateOverrideRpmsFromDefaults(self, defaults, verbose = False):
		def showOverrideSet(how, overrideSet):
			if not overrideSet:
				infomsg(f"    {how}: none")
			else:
				infomsg(f"    {how}:")
				for entry in overrideSet:
					infomsg(f"     - {entry}")

		def show(msg, includeSet, excludeSet):
			infomsg(f"  {msg}:")
			showOverrideSet("include", includeSet)
			showOverrideSet("exclude", excludeSet)
			infomsg('')

		incrementInclude = defaults._overrideRpmsInclude.difference(self._overrideRpmsExclude)
		incrementExclude = defaults._overrideRpmsExclude.difference(self._overrideRpmsInclude)

		if verbose:
			infomsg(f"{self} updating overrides from {defaults}:")
			show("configured", self._overrideRpmsInclude, self._overrideRpmsExclude)
			show("increment", incrementInclude, incrementExclude)

		self._overrideRpmsInclude.update(incrementInclude)
		self._overrideRpmsExclude.update(incrementExclude)

		if verbose:
			show("result", self._overrideRpmsInclude, self._overrideRpmsExclude)

	def overrideRpmInclude(self, rpmOverrideList):
		self._overrideRpmsInclude = rpmOverrideList

	def overrideRpmExclude(self, rpmOverrideList):
		self._overrideRpmsExclude = rpmOverrideList

class Composer(object):
	def __init__(self, classificationScheme, includeExplanations = False, verbose = True):
		self.classificationScheme = classificationScheme
		self.includeExplanations = includeExplanations
		self.verbose = verbose

		self._release = None
		self.defaultLifecycle = None

		self.useCaseCatalog = None
		self.signoffs = None
		self.productDefaults = None
		self._products = {}

		# we deal with multiple products, so we need to differentiate their
		# properties by using a scenario called "product". In particular, we
		# handle $foo-release packages using product/release
		self.releaseScenario = "product/release"

		self._epicProductMembership = DictOfSets()
		self._rpmProductMembership = DictOfSets()
		self._buildProductMembership = DictOfSets()

	@property
	def products(self):
		return iter(self._products.values())

	def bottomUpProductTraversal(self):
		return sorted(self.products, key = ProductComposition.sortkey)

	@property
	def release(self):
		return self._release

	@release.setter
	def release(self, id):
		release = self.classificationScheme.policy.getRelease(id)
		if release is None:
			raise Exception(f"invalid release id {id}")

		self._release = release

	def createProduct(self, id):
		if id in self._products:
			raise Exception(f"Duplicated definition of product {id}")

		product = ProductComposition(id, self.classificationScheme)
		if id == 'defaults':
			self.productDefaults = product
		else:
			self._products[id] = product
		return product

	def lookupProduct(self, id):
		return self._products.get(id)

	def loadProductComposition(self, path):
		from .new_compose import CompositionBuilder

		spec = CompositionBuilder.load(self.classificationScheme, path)
		infomsg(f"Loading composition spec; translate to composer classes now")

		self.release = spec.release

		for productSpec in spec.products:
			product = self.createProduct(productSpec.id)
			if productSpec.name is not None:
				product.name = productSpec.name
			product.architectures = productSpec.architectures
			product.baseProductName = productSpec.baseProductName
			product.type = productSpec.type
			product.contractNames = productSpec.contractNames
			product.obsComposeKey = productSpec.obsComposeKey
			product.releasePackage = productSpec.releasePackage
			product.releaseEpic = productSpec.releaseEpic
			product.rules = productSpec.rules

			if not product.architectures:
				raise Exception(f"{product}: cannot determine supported architectures")

			if productSpec._overrideRpmInclude:
				product.overrideRpmInclude(productSpec._overrideRpmInclude)

			if productSpec._overrideRpmExclude:
				product.overrideRpmExclude(productSpec._overrideRpmExclude)

			if product.baseProductName is not None:
				product.baseProduct = self._products[product.baseProductName]
				if not product.architectures:
					product.architectures = product.baseProduct.architectures

				product.updateOverrideRpmsFromDefaults(product.baseProduct)

			if product.type == product.TYPE_EXTENSION and \
			   not product.baseProductName:
				raise Exception(f"extension {product} lacks a base product name (spec={productSpec}; {productSpec.baseProductName})")

		return

	def compose(self, classificationResult):
		# We could probably use the value from the --release command line option here
		if self.release is None:
			raise Exception(f"You need to specify a release id for this product group")

		self.resolveLifecycles()

		report = GenericStringReport("Detected the following problems in composer definition")
		self.composePackages(report, classificationResult)

		self.errorReport = report

		# HACK: some of the producers want to go back to the classificationResult.
		# The proper way to do this would be for this function to return a ReleaseComposition
		# object, which would contain all the ProductComposition objects plus the classification
		self.classificationResult = classificationResult

	def displayRpmDecisions(self, db):
		CompositionResultLogger.displayRpmDecisions(db, self.products)

	def writeYamlAll(self, outputPath):
		producer = YamlAllProducer()
		producer.produce(self)
		producer.write(outputPath)

	def writeYamlProductComposer(self, outputPath, inputPath):
		producer = YamlOBSProductComposerProducer(inputPath)
		producer.produce(self)
		producer.write(outputPath)

	def writeYamlGroupsYaml(self, outputPath):
		producer = YamlOBSGroupsYamlProducer()
		producer.produce(self)
		producer.write(outputPath)

	def writeYamlComponents(self, outputPath):
		producer = YamlComponentsProducer()
		producer.produce(self)
		producer.write(outputPath)

	def writeYamlLifecycles(self, outputPath):
		producer = YamlLifecycleProducer()
		producer.produce(self)
		producer.write(outputPath)

	def writeZypperLifecycles(self, outputPath):
		producer = ZypperLifecycleProducer()
		producer.produce(self)
		producer.write(outputPath)

	def writeSupportStatus(self, outputPath):
		producer = SupportStatusProducer()
		producer.produce(self)
		producer.write(outputPath)

	def composePackages(self, report, classificationResult):
		fullArchSet = archRegistry.fullset

		for product in self.products:
			archSet = ArchSet(product.architectures)
			if not archSet:
				report.add(f"{product}: you need to specify the list of supported architectures")
				continue

			if not archSet.issubset(fullArchSet):
				missing = fullArchSet.difference(archSet)
				report.add(f"{product}: codebase lacks support for architecture(s) {' '.join(missing)}")

			if self.verbose:
				infomsg(f"Applying composition rules for product {product}")
			product.rules.apply(classificationResult)

			product.rpms = product.rules.produceSolution(classificationResult)
			product.supportStatement = product.rules.produceSupportSummary(classificationResult)

			if self.includeExplanations:
				product.reasoning = product.rules.produceReasoning(classificationResult)

			self.overrideRpms(product, report, classificationResult)

			self.resolveReleasePackages(product, classificationResult)

			self.verifyPromises(product, report, classificationResult)

			product.rpms.update(product.releaseRpms)

			for rpm in product.rpms:
				if rpm.new_class and rpm.new_class.isIgnored:
					report.add(f"{product} includes rpm {rpm}, which must not be shipped (class={rpm.new_class})")

		# This is not exceptionally useful yet because we cannot say "ignore this build failure for now"
		# self.verifyBuilds(report)

		for product in self.products:
			if product.baseProduct is not None and product.type == product.TYPE_EXTENSION:
				product.rpms.difference_update(product.baseProduct.rpms)
				product.releaseRpms.difference_update(product.baseProduct.rpms)

	def resolveReleasePackages(self, product, classificationResult):
		if product.releasePackage is not None:
			# FIXME: look this up in the DB
			releaseRpm = GenericRpm(product.releasePackage)
			product.releaseRpms.add(releaseRpm)

		if product.releaseEpic is not None:
			epic = self.classificationScheme.nameToEpic(product.releaseEpic)

			members = product.rules.resolveIncrementalEpic(epic, classificationResult)
			if not members:
				raise Exception(f"{product}: release epic {epic} resolves to empty package list")

			if self.verbose:
				infomsg(f"{product}: using release/product packages from {epic}: {' '.join(map(str, members))}")
			product.releaseRpms.update(members)

		if product.releaseRpms and self.releaseScenario:
			product.releaseScenario = classificationResult._db.lookupRpm(self.releaseScenario)

	def verifyPromises(self, product, report, classificationResult):
		validator = PromiseValidator(product, verbose = self.verbose)

		productArchitectures = ArchSet(product.architectures)

		for rpm in validator.rpmsDependingOnScenario:
			supportedScenarios = set()
			validForScenarios = rpm.getValidScenarios(productArchitectures)

			for arch in product.getRpmArchitectures(rpm):
				requiredScenarios = rpm.validScenarios.get(arch)
				if not requiredScenarios:
					continue

				supportedScenarios = set()
				for s in requiredScenarios:
					check = set(s.split('|'))

					if check.issubset(validator._allSupportedScenarios):
						supportedScenarios.add(s)

				if not supportedScenarios:
					report.add(f"{product}/{arch}: dependencies of {rpm} cannot be satisfied")
					report.add(f"   {rpm} is valid in these scenarios: {' '.join(requiredScenarios)}")
					report.add(f"   but none of these is supported by the product")

		for rpm in validator.rpmsWithConditionalDependencies:
			db = classificationResult.db

			for string in rpm.conditionals.common:
				validator.validateConditionalRequirement(db, rpm, string, report)
			for arch, depSet in rpm.conditionals.items():
				for string in depSet.difference(rpm.conditionals.common):
					validator.validateConditionalRequirement(db, rpm, string, report, arch)

	def overrideRpms(self, product, report, classificationResult):
		excludeRpms = product._overrideRpmsExclude.toRpms(classificationResult.db)
		includeRpms = product._overrideRpmsInclude.toRpms(classificationResult.db)

		if excludeRpms:
			self.excludeRpms(excludeRpms, product, report)
		if includeRpms:
			self.includeRpms(includeRpms, product, report)

	def excludeRpms(self, excludeRpms, product, report):
		if self.verbose:
			infomsg(f"{product} override exclude:")
			for rpm in excludeRpms:
				if rpm not in product.rpms:
					infomsg(f" - {rpm} {rpm.architectures} [ALREADY DROPPED FROM COMPOSITION]")
				else:
					infomsg(f" - {rpm}")

		product.rpms.difference_update(excludeRpms)

		if product.reasoning is not None:
			for rpm in excludeRpms:
				product.reasoning.overrideExclude(rpm)

	def includeRpms(self, includeRpms, product, report):
		if self.verbose:
			infomsg(f"{product} override include:")
			for rpm, archSet in includeRpms.rpmsWithArch():
				if rpm not in product.rpms:
					infomsg(f" - {rpm} {archSet}")
				else:
					haveArch = product.rpms.getArch(rpm.name) or rpm.architectures
					if haveArch == archSet:
						infomsg(f" - {rpm} {archSet} [ALREADY PART OF COMPOSITION]")
					else:
						infomsg(f" - {rpm} {archSet} (was: {haveArch})")

		for rpm, archSet in includeRpms.rpmsWithArch():
			product.rpms.add(rpm, archSet, overwriteArch = True)

		if product.reasoning is not None:
			for rpm, archSet in includeRpms.rpmsWithArch():
				product.reasoning.overrideInclude(rpm, archSet)

		availableRpms = product.rpms
		if product.baseProduct is not None:
			availableRpms = availableRpms.union(product.baseProduct.rpms)

		for rpm in includeRpms:
			requiredRpms = set(rpm.enumerateRequiredRpms())
			if not requiredRpms.issubset(availableRpms):
				report.add(f"{product}: override rules require addition of rpm {rpm}, but not all of its requirements are provided")
				for req in requiredRpms:
					if req not in availableRpms:
						report.add(f" - {req}")

	def verifyBuilds(self, report):
		for buildInfo, products in sorted(self._buildProductMembership.items(), key = lambda pair: str(pair[0])):
			if not buildInfo.buildIssues:
				continue

			problems = []

			for name, arch, status in buildInfo.buildIssues:
				if status != 'excluded':
					problems.append(f"{name} {arch} {status}")

			if problems:
				report.add(f"build {buildInfo} (used by products {' '.join(map(str, products))}")
				for detail in problems:
					report.add(f"   {detail}")

	def resolveLifecycles(self):
		policy = self.classificationScheme.policy
		release = self.release

		id = release.lifecycle
		if id is None:
			raise Exception(f"Release {release} does not specify a default lifecycle")

		defaultLifecycle = policy.getLifeCycle(id)
		if defaultLifecycle is None:
			raise Exception(f"Release {release} specifies unknown default lifecycle {id}")

		defaultLifecycle.updateContractsFromRelease(release)
		self.defaultLifecycle = defaultLifecycle

		for lifecycle in policy.lifecycles:
			if lifecycle.releaseDate is None:
				lifecycle.updateContractsFromRelease(release)

			if self.verbose:
				infomsg(f"   {lifecycle} release {lifecycle.releaseDate}")
				for contract in lifecycle.contracts:
					if contract.enabled:
						infomsg(f"      {contract} duration={contract.duration} end={contract.endOfSupport}")

class PromiseValidator(object):
	def __init__(self, product, verbose = False):
		self.product = product
		self.verbose = verbose
		self.rpmsDependingOnScenario = set()
		self.regularRpms = set()
		self._allSupportedScenarios = ScenarioTupleSet()

		self.allVisibleRpms = product.allAvailableRpms
		self.rpmsWithConditionalDependencies = set()

		self.addProduct(product)

	def addProduct(self, product, parentProduct = None):
		if self.verbose:
			if parentProduct:
				infomsg(f"adding {product} (in the context of {parentProduct}))")
			else:
				infomsg(f"adding {product}")

		isMainProduct = (parentProduct is None)

		productRpms = product.rpms

		if product.releaseScenario is not None:
			if self.verbose:
				infomsg(f"Add scenario {product.releaseScenario} to product {product} using rpm(s) {' '.join(map(str, product.releaseRpms))}")
			productRpms = productRpms.union(product.releaseRpms)

		for rpm in productRpms:
			if not rpm.isSynthetic:
				self.regularRpms.add(rpm)

				self._allSupportedScenarios.update(rpm.controllingScenarios.common)

				for req in rpm.enumerateRequiredRpms():
					if req.type == req.TYPE_PROMISE:
						raise Exception(f"Should not happen: {rpm} requires promise {req}")

				# Build the queue of rpms to inspect only for the product
				# we're currently inspecting; not for its base product.
				if isMainProduct and rpm.validScenarios.common:
					self.rpmsDependingOnScenario.add(rpm)

				if isMainProduct and rpm.conditionals:
					self.rpmsWithConditionalDependencies.add(rpm)

		if product.baseProduct is not None and product.type == product.TYPE_EXTENSION:
			with loggingFacade.temporaryIndent():
				self.addProduct(product.baseProduct, parentProduct = product)

	def validateConditionalRequirement(self, db, rpm, string, report, arch = None):
		try:
			conditional = BooleanDependency.parseCompiled(string)
		except Exception as e:
			errormsg(f"{rpm}: cannot parse conditional dependency {string} (exception {e})")
			return

		if arch:
			rpmid = f"{rpm}.{arch}"
		else:
			rpmid = rpm.name

		if rpm.trace:
			infomsg(f"{self.product}/{rpmid}: verifying the conditional dependency {string} can be resolved")

		for assertion in conditional.permutations():
			dependingOn = set(map(db.lookupRpm, assertion.include))
			if not dependingOn.issubset(self.allVisibleRpms):
				lacking = dependingOn.difference(self.allVisibleRpms)
				if rpm.trace:
					infomsg(f"ignore conditional {assertion.asNode()}: product {self.product} does not include {' '.join(map(str, lacking))}")
				continue

			satisfied = False
			for impl in assertion.solutions:
				required = set(map(db.lookupRpm, impl.include))
				if required.issubset(self.allVisibleRpms):
					if rpm.trace:
						infomsg(f"can satisfy conditional {impl.asNode()}")
					satisfied = True
					break

			if satisfied:
				continue

			report.add(f"{self.product}: conditional dependency {string} of {rpmid} cannot be satisfied")
			for impl in assertion.solutions:
				missing = set(map(db.lookupRpm, impl.include)).difference(self.allVisibleRpms)
				report.add(f"   {impl.asNode()}: missing {' '.join(map(str, missing))}")

class YamlAllProducer(YamlDictProducerBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def produce(self, composer):
		root = self.DictNode(addExtraSpacing = True)
		for product in composer.bottomUpProductTraversal():
			data = root.createDict(product.id)

			self.produceProduct(product, data)

		self.root = root

	def produceProduct(self, product, data):
		data.createScalar('name', product.name)
		data.createList('architectures', sorted(product.architectures))
		data.createList('contracts', product.contractNames)
		data.addEntry('rpms', self.RpmList(product.rpms))

class YamlComponentsProducer(YamlDictProducerBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def produce(self, composer):
		view = EpicCentricView(composer)

		root = self.DictNode(addExtraSpacing = True)
		for epic in view:
			self.produceEpic(epic, root, view)

		self.root = root

	def produceEpic(self, epic, root, view):
		composer = view.composer
		memberRpms = view.epicRpms[epic]

		shippedRpms = view.allRpms.intersection(memberRpms)
		if not shippedRpms:
			extra = [str(epic)]

			team = composer.classificationScheme.policy.getTeam(epic.maintainerID)
			if team is not None:
				extra.append(f"reviewer {str(team)}")

			extra.append(f"({len(memberRpms)} rpms)")

			root.addComment(f"not shipped: {'; '.join(extra)}")
			return

		data = root.createDict(epic.name)

		if epic.description:
			data.createScalar('description', epic.description)

		if epic.maintainerID is not None:
			team = composer.classificationScheme.policy.getTeam(epic.maintainerID)
			if team is not None:
				data.createScalar('reviewer', str(team))
			else:
				raise Exception(f"Bad maintainer {epic.maintainerID} in epic {epic}")
				data.createScalar('reviewer', epic.maintainerID)

		if epic.lifecycleID is not None:
			data.createScalar('lifecycle', epic.lifecycleID)

		if composer.signoffs is not None:
			# FIXME: we need to take release into account
			signoff = composer.signoffs.lookupEpic(epic)
			if signoff is not None:
				data.createScalar('signoff', f"{signoff.owner} {signoff.date} ({signoff.hash})")

		epicProductMembership = composer._epicProductMembership[epic]
		if epicProductMembership:
			# for some epics, we just ship the stuff that's in a build option, and that's it
			productData = data.createList('products')
			for productName in sorted(map(str, epicProductMembership)):
				productData.addEntry(productName)

		memberBuilds = view.epicPackages[epic]
		if memberBuilds:
			useCaseIndex = view.getUsecaseIndexForBuildList(memberBuilds)
			if useCaseIndex:
				useCaseData = data.createDict('usecases')
				for key, value in sorted(useCaseIndex.items()):
					useCaseData.createScalar(key, ' '.join(sorted(value)))

			packageList = data.createList('builds')
			for build in sorted(memberBuilds, key = str):
				if not build.isSynthetic:
					packageList.addEntry(build.name)


		rpmList = self.RpmProductList(shippedRpms, defaultProducts = epicProductMembership,
							lookupMembership = composer._rpmProductMembership.get)

		shippedList = data.addEntry('public_rpms', rpmList)

		internalRpms = memberRpms.difference(view.allRpms)
		if internalRpms:
			unshippedList = data.addEntry('private_rpms', self.RpmList(internalRpms, format = 'plain'))

	class RpmProductList(YamlDictProducerBase.RpmList):
		def __init__(self, *args, defaultProducts = None, lookupMembership = None, **kwargs):
			super().__init__(*args, **kwargs)
			self.defaultProducts = defaultProducts
			self.lookupMembership = lookupMembership

		def render(self, listFormatter):
			for rpm in sorted(self.rpms, key = str):
				if self.lookupMembership is not None:
					products = self.lookupMembership(rpm)
					if products == self.defaultProducts:
						listFormatter.add(f"{rpm.name}")
					else:
						productNames = sorted(map(str, products))
						listFormatter.add(f"{rpm.name}: [{','.join(productNames)}]")

class YamlOBSProductComposerProducer(YamlDictProducerBase):
	def __init__(self, templatePath, *args, **kwargs):
		super().__init__(*args, **kwargs)

		self.templatePath = templatePath

	def renderHeader(self, ioStream):
		with open(self.templatePath) as f:
			for line in f.readlines():
				line = line.rstrip()
				print(line, file = ioStream)
				if 'The following is generated by openSUSE-release-tools' in line:
					break

	def render(self, ioStream):
		listFormatter = self.createListFormatter(ioStream, indent = "")
		self.root.render(listFormatter)

	def produce(self, composer):
		mainList = []

		root = self.ListNode(addExtraSpacing = True)
		for product in composer.bottomUpProductTraversal():
			self.produceProduct(product, root, mainList)

		offline = ProductComposition('sles_offline', product.classificationScheme)
		offline.architectures = archRegistry.fullset
		for product in composer.products:
			offline.rpms.update(product.rpms)
		self.produceProduct(offline, root, mainList)

		self.createMainList(root, 'main', mainList)

		self.root = root

	# The way these flavors reference each other looks very weird...
	# Hopefully this is correct
	def produceProduct(self, product, root, mainList):
		name = product.obsComposeKey
		if name is None:
			name = product.id.rstrip("_-.0123456789")

		productArchitectures = ArchSet(product.architectures)

		defaultFlavorName = name

		allFlavorNames = [defaultFlavorName]
		for arch in sorted(product.architectures):
			allFlavorNames.append(f"{name}_{arch}")

		flavorMap = {}

		flavorMap[None] = self.createFlavor(root, defaultFlavorName, allFlavorNames, None)

		mainList.append(name)
		for arch in sorted(product.architectures):
			archFlavorName = f"{name}_{arch}"
			flavorMap[arch] = self.createFlavor(root, archFlavorName,
						[defaultFlavorName, archFlavorName],
						[arch])
			mainList.append(archFlavorName)

		rpmListMap = {}
		for key, node in flavorMap.items():
			rpmListMap[key] = node.addEntry('packages', self.RpmList())

		for rpm, rpmArchitectures in product.regularRpmsWithArch:
			if rpm.type != rpm.TYPE_REGULAR:
				raise Exception(f"{rpm}: not a regular rpm")
			rpmArchitectures = rpmArchitectures.intersection(productArchitectures)
			if productArchitectures == rpmArchitectures:
				# the rpm is available for all the product's architectures.
				rpmListMap[None].add(rpm)
			else:
				for arch in rpmArchitectures:
					rpmListMap[arch].add(rpm)

		# Add packages such as $product-release
		# These need to be added here rather than the entire body of packages for the
		# given product, because these packages should not be inherited by another
		# (base) product that sits on top of this one
		for rpm in product.releaseRpms:
			rpmListMap[None].add(rpm)

	def createFlavor(self, root, name, flavors, architectures, supportStatus = 'l3'):
		entry = root.addDict()

		entry.createScalar('name', name)
		entry.createScalar('supportstatus', supportStatus)
		entry.createList('flavors', flavors)
		if architectures:
			entry.createList('architectures', sorted(architectures))

		return entry

	def createMainList(self, root, name, flavors):
		entry = root.addDict()

		entry.createScalar('name', name)
		entry.createList('add', flavors)

class YamlOBSGroupsYamlProducer(YamlDictProducerBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def produce(self, composer):
		root = self.DictNode(addExtraSpacing = True)
		for product in composer.bottomUpProductTraversal():
			self.produceProduct(product, root)

		self.root = root

	# The way these flavors reference each other looks very weird...
	# Hopefully this is correct
	def produceProduct(self, product, root):
		name = product.obsComposeKey
		if name is None:
			name = product.id.rstrip("_-.0123456789")

		productArchitectures = ArchSet(product.architectures)
		productNode = root.createList(name)

		for rpm, rpmArchitectures in locale_sorted(product.regularRpmsWithArch, key = lambda pair: str(pair[0])):
			if rpm.type != rpm.TYPE_REGULAR:
				raise Exception(f"{rpm}: not a regular rpm")
			rpmArchitectures = rpmArchitectures.intersection(productArchitectures)
			if productArchitectures == rpmArchitectures:
				# the rpm is available for all the product's architectures.
				productNode.addEntry(f"{rpm}")
			else:
				archList = sorted(rpmArchitectures)
				productNode.addEntry(f"{rpm}: [{','.join(archList)}]")

class YamlLifecycleProducer(YamlMultiDictProducerBase):
	class Roadmap(object):
		def __init__(self):
			self.releaseDates = {}

		def addReleaseDate(self, id, release):
			if release is None or release.date is None:
				return

			date = release.date
			if date.month <= 6:
				date = f"{date.year}/H1"
			else:
				date = f"{date.year}/H2"

			self.releaseDates[id] = date

		def getReleaseDate(self, id):
			return self.releaseDates.get(id)

	def createRoadmap(self, composer):
		roadmap = self.Roadmap()

		release = composer.release
		policy = composer.classificationScheme.policy

		for id in ('tick', 'tock'):
			nextRelease = policy.getSubsequentRelease(release, id)
			if nextRelease is None:
				errormsg(f"Cannot create tick tock roadmap info for release {release} - no upcoming {id} release")
				return None
			roadmap.addReleaseDate(id, nextRelease)

		return roadmap

	def produce(self, composer):
		self.roadmap = self.createRoadmap(composer)

		for product in composer.bottomUpProductTraversal():
			data = self.createDocument(product.id)
			self.produceProduct(data, product, composer)

	def produceProduct(self, parent, product, composer):
		data = parent.createDict('__info__')
		data.createScalar('schema', '1.0')
		self.produceProductInfo(data, product)

		view = LifecycleCentricView(composer, product)

		if view.defaultLifecycle is not None:
			data.createScalar('default_lifecycle', view.defaultLifecycle.id)
			self.produceLifecycle(parent, view.defaultLifecycle, view.defaultRpms)

		for lifecycle, rpms in view:
			if lifecycle is view.defaultLifecycle:
				continue
			self.produceLifecycle(parent, lifecycle, rpms)

	def produceProductInfo(self, parent, product):
		data = parent.createDict('product')
		data.createScalar('id', product.id)
		data.createScalar('name', product.name)
		data.createScalar('type', product.type)
		if product.baseProduct is not None:
			data.createScalar('base_product', product.baseProduct.id)

	def produceLifecycle(self, parent, lifecycle, rpms):
		data = parent.createDict(lifecycle.id)

		# Do not display release/EOL dates for life cycles that have
		# implementations (such as generic openjdk, rust, gcc, etc)
		displayDates = not(lifecycle.implementations)

		if lifecycle.displayName == lifecycle.id:
			warnmsg(f"lifecycle {lifecycle}: no display_name set")
		if lifecycle.displayName:
			data.createScalar('display_name', lifecycle.displayName)
		if lifecycle.displayHint:
			data.createScalar('display_hint', lifecycle.displayHint)
		if lifecycle.description:
			data.createScalar('description', lifecycle.description)
		data.createScalar('mode', str(lifecycle.mode))

		if lifecycle.cadence:
			data.createScalar('cadence', str(lifecycle.cadence))

		if lifecycle.stability:
			data.createScalar('stability', str(lifecycle.stability))

		if lifecycle.cadence == lifecycle.CADENCE_TICKTOCK and self.roadmap is not None:
			self.produceRoadmap(data, ('tick', 'tock'))

		if lifecycle.releaseDate and displayDates:
			data.createScalar('release', str(lifecycle.releaseDate))
		if lifecycle.implementations:
			data.createList('implementations', sorted(map(str, lifecycle.implementations)))
		for contract in lifecycle.contracts:
			cdata = data.createDict(contract.id)
			if not contract.enabled:
				cdata.createScalar('supported', 'no')
				continue
			cdata.createScalar('supported', 'yes')
			if contract.stability and contract.stability != lifecycle.stability:
				cdata.createScalar('stability', str(contract.stability))
			if contract.endOfSupport is not None and displayDates:
				cdata.createScalar('end', str(contract.endOfSupport))

		if rpms:
			data.createList('rpms', sorted(map(str, rpms)))

	def produceRoadmap(self, parent, tags):
		data = parent.createDict('roadmap')
		for id in tags:
			date = self.roadmap.getReleaseDate(id)
			if date:
				data.createScalar(f"next_{id}_release", date)

##################################################################
# Generate the tables for zypper-lifecycle-plugin
##################################################################
class ZypperLifecycleProducer(object):
	class ZypperLifecycleTable(object):
		def __init__(self, product, contractName = None):
			self.productName = product.obsComposeKey or product.name
			self.contractName = contractName
			self.rows = []

		@property
		def id(self):
			if self.contractName is None:
				return self.productName
			return f"{self.productName}-{self.contractName}"

		def add(self, rpm, eolDate):
			self.rows.append((rpm.name, '*', eolDate))

	def __init__(self):
		self.tables = []

	def produce(self, composer):
		if composer.defaultLifecycle is None:
			return

		defaultLifecycle = composer.defaultLifecycle

		for product in composer.bottomUpProductTraversal():
			view = LifecycleCentricView(composer, product)

			for contract in defaultLifecycle.contracts:
				if not contract.enabled:
					continue

				table = self.createTable(composer, product, contract)

				# infomsg(f"Building zypper data for {product} with contract {contract}: id={table.id}")
				self.produceProduct(table, view, contract)

	def createTable(self, composer, product, contract):
		contract = composer.defaultLifecycle.getContract(contract.id)
		if contract is composer.defaultLifecycle.generalSupport:
			table = self.ZypperLifecycleTable(product)
		else:
			table = self.ZypperLifecycleTable(product, contract.id)
		self.tables.append(table)
		return table

	def produceProduct(self, table, view, defaultContract):
		contractID = defaultContract.id

		for lifecycle, rpms in view:
			contract = lifecycle.getContract(contractID)
			if not contract.enabled:
				continue

			if contract.endOfSupport == defaultContract.endOfSupport:
				continue

			for rpm in rpms:
				# infomsg(f"  {table.id} {rpm} {contract.endOfSupport}")
				table.add(rpm, contract.endOfSupport)

	def write(self, outputPathTemplate):
		if '%id' not in outputPathTemplate:
			raise Exception(f"{self.__class__.__name__}: output path \"{outputPathTemplate}\" does not contain '%id'")

		infomsg(f"Writing zypper life cycle tables:")
		for table in self.tables:
			outputPath = outputPathTemplate.replace('%id', table.id)

			with loggingFacade.temporaryIndent():
				self.writeTable(outputPath, table)

	def writeTable(self, outputPath, table):
		csv = CSVWriter(outputPath)

		for row in sorted(table.rows):
			csv.write(row)

		csv.close()

class SupportStatusProducer(object):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.data = None

	def produce(self, composer):
		supportDictionary = composer.classificationScheme.policy.supportDictionary
		if supportDictionary is None:
			infomsg(f"Model does not define support levels")
			return

		self.data = {}
		for product in composer.bottomUpProductTraversal():
			if product.supportStatement is not None:
				self.data[product.id] = product.supportStatement

		self.defaultLevel = supportDictionary.defaultLevel

	def write(self, outputPathTemplate):
		if self.data is None:
			return

		if '%id' not in outputPathTemplate:
			raise Exception(f"{self.__class__.__name__}: output path \"{outputPathTemplate}\" does not contain '%id'")

		for id, map in self.data.items():
			outputPath = outputPathTemplate.replace('%id', id)

			infomsg(f"Writing {outputPath}")
			with open(outputPath, "w") as f:
				for rpm, level in sorted(map.items(), key = lambda pair: str(pair[0])):
					if level is not self.defaultLevel:
						print(f"{rpm:30} {level}", file = f)

##################################################################
# Helper class to provide an epic centric view of all products
##################################################################
class EpicCentricView(object):
	def __init__(self, composer):
		self.composer = composer

		self.allRpms = set()
		for product in composer.bottomUpProductTraversal():
			self.allRpms.update(product.rpms)

		self.epicRpms = {}
		for epic in composer.classificationScheme.allEpics:
			self.epicRpms[epic] = set()

		self.epicPackages = {}
		for epic in composer.classificationScheme.allEpics:
			self.epicPackages[epic] = set()

		db = composer.classificationResult.db

		for build in db.builds:
			epic = build.new_epic
			if epic is None:
				continue

			self.epicPackages[epic].add(build)
			self.epicRpms[epic].update(build.rpms)

	def __iter__(self):
		epicOrder = self.composer.classificationScheme.epicOrder()
		for epic in epicOrder.bottomUpTraversal():
			# If the epic does not contain any rpms at all, dont bother mentioning it.
			memberRpms = self.epicRpms[epic]
			if not memberRpms:
				continue

			if all(rpm.isSynthetic for rpm in memberRpms):
				continue

			yield epic

	def getUsecaseForBuild(self, name):
		catalog = self.composer.useCaseCatalog
		if catalog is None:
			return None

		uci = catalog.lookupBuild(name)
		if uci is None and ':' in name:
			name = name.split(':')[0]
			uci = catalog.lookupBuild(name)

		return uci

	def getUsecaseIndexForBuildList(self, buildList):
		result = {}
		for build in buildList:
			uci = self.getUsecaseForBuild(build.name)
			if uci is None:
				continue

			key = uci.slug
			if key not in result:
				result[key] = set((build.name,))
			else:
				result[key].add(build.name)

		return result

##################################################################
# Helper class to provide a life cycle based view on a product
##################################################################
class LifecycleCentricView(object):
	def __init__(self, composer, product):
		self.composer = composer

		policy = composer.classificationScheme.policy
		self.defaultLifecycle = composer.defaultLifecycle

		self.members = {}
		self.alwaysShown = set()
		for lifecycle in policy.lifecycles:
			self.members[lifecycle] = set()

			if lifecycle.implementations:
				self.alwaysShown.add(lifecycle)

		for rpm in product.rpms:
			if rpm.isSynthetic:
				continue

			hints = rpm.labelHints
			if hints is not None and hints.lifecycleID:
				lifecycle = policy.getLifeCycle(hints.lifecycleID)
				if lifecycle is None:
					raise Exception(f"rpm {rpm}: lifecycle {hints.lifecycleID} not known")
			else:
				epic = rpm.new_build.new_epic
				if epic is None:
					continue

				lifecycle = policy.getLifeCycle(epic.lifecycleID)
				if lifecycle is None:
					raise Exception(f"{epic}: lifecycle {epic.lifecycleID} not known")

			self.members[lifecycle].add(rpm)

			implementedLifecycle = lifecycle.implements
			if implementedLifecycle is not None:
				self.alwaysShown.add(implementedLifecycle)

	def __iter__(self):
		for lifecycle, rpms in self.members.items():
			if rpms or lifecycle in self.alwaysShown:
				yield lifecycle, rpms

	@property
	def defaultRpms(self):
		return self.members.get(self.defaultLifecycle) or []

##################################################################
# Helper class for tracing
##################################################################
class CompositionResultLogger(object):
	@staticmethod
	def displayRpmDecisions(db, products):
		tracedRpms = set(filter((lambda rpm: rpm.trace), db.rpms))
		if not tracedRpms:
			return

		def writeLine(words):
			msg = f"{words[0]:{col0w}}"
			for w in words[1:]:
				msg += f" {w:{col1w}}"
			infomsg(msg)

		# Convert iter to sorted list
		products = sorted(products, key = str)

		col0w = max(len(rpm.name) for rpm in tracedRpms) + 2
		col1w = max(len(product.name) for product in products) + 2

		infomsg("")
		infomsg(f"State of traced rpms:")
		writeLine([""] + [product.name for product in products])
		for rpm in tracedRpms:
			if not rpm.trace:
				continue

			words = [rpm.name]
			for product in products:
				if rpm in product.rpms:
					words.append('yes')
				else:
					words.append('-')
			writeLine(words)
		infomsg("")

