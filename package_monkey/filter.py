##################################################################
#
# Label hierarchy used to abstract package dependencies
#
##################################################################
import fnmatch
import datetime
from functools import reduce

from .util import ExecTimer, TimedExecutionBlock
from .util import loggingFacade, debugmsg, infomsg, warnmsg, errormsg
from .ordered import PartialOrder
from .pmatch import ParallelStringMatcher
from .profile import profiling
from .policy import Policy
from .reports import LocationIndexedReport
from .arch import *

# hack until I'm packaging fastsets properly
import fastset as fastsets

initialPlacementLogger = loggingFacade.getLogger('initial')
debugInitialPlacement = initialPlacementLogger.debug

# Set this to True if you want to enable (somewhat expensive) checks while
# updating dependencies.
enableSafeDependencyUpdates = True
enableSafeDependencyUpdates = False

class Classification(object):
	TYPE_EPIC = 'epic'
	TYPE_LAYER = 'layer'
	TYPE_AUTOFLAVOR = 'autoflavor'
	TYPE_EXTRA = 'extra'
	TYPE_CLASS = 'class'
	TYPE_BUILD_OPTION = 'buildoption'

	VALID_TYPES = (
		TYPE_EPIC,
		TYPE_LAYER,
		TYPE_AUTOFLAVOR,
		TYPE_EXTRA,
		TYPE_CLASS,
		TYPE_BUILD_OPTION,
	)

	# should this be a member of the classification scheme?
	domain = fastsets.Domain("label")

	class LabelCategory(object):
		def __init__(self, type, classificationScheme):
			self.type = type
			self.classificationScheme = classificationScheme

			self.frozen = False
			self.allLabels = Classification.createLabelSet()

			labelClass = Classification.labelTypeToClass.get(self.type)
			if labelClass is not None:
				assert(isinstance(labelClass, Classification.Label))
			else:
				labelClass = Classification.Label
			self.labelClass = labelClass

			self._map = {}

		def __str__(self):
			return self.name

		@property
		def name(self):
			return str(self.type)

		def freeze(self):
			self.frozen = True

		def lookup(self, name):
			return self._map.get(name)

		def createLabel(self, name, **kwargs):
			if name in self._map:
				fart

			label = self.labelClass(name, self.type, **kwargs)

			self.allLabels.add(label)
			self._map[name] = label
			return label

	class Label(domain.member):
		def __init__(self, name, type, parent = None):
			super().__init__()

			self.name = name
			self.type = type
			self.parent = parent
			self.layer = None
			self.epic = None
			self.description = None
			self.runtimeRequires = Classification.createLabelSet()
			self.configuredRuntimeRequires = Classification.createLabelSet()
			self.requiredOptions = Classification.createLabelSet()

			if parent is not None:
				if parent.type is Classification.TYPE_EPIC:
					self.setEpic(parent)
				elif parent.epic is not None:
					self.setEpic(parent.epic)
				else:
					raise Exception(f"Refusing to create {self} as child of {parent}: parent not associated with any epic")

			self.defined = False

			# This is populated for labels that represent a build flavor like @Core+python,
			# or a class topic like @Core-devel, or a flavor AND class, like @Core+python-devel
			self.flavorName = None
			self.fromAutoFlavor = None

			# This is populated for base flavors like @Core
			self._flavors = {}

			# binary labels may be part of a build option definition
			self.definingBuildOption = None

			# for a component label, this will hold the set of topic labels that
			# belong to this component.
			# It will be set after the topic tree has been frozen
			self._referencingLabels = Classification.createLabelSet()

			# Set this to true if you want to ignore all associated packages
			# and their dependencies.
			# Currently only available for class labels
			self.isIgnored = False

			self.decisionLog = []

			self.definingLocation = None

			# policy objects defined for this label
			self.lifecycleID = None
			self.maintainerID = None

			# the set of architectures this label is valid for
			self._archSet = None

			# If tracing is enabled for this label
			self.trace = False

		@property
		def fingerprint(self):
			values = [self.name, self.type, self.isIgnored]
			for attrName in ('_flavors', 'runtimeRequires'):
				values.append(attrName)

				attr = getattr(self, attrName)

				# some of these are label valued dicts
				if type(attr) == dict:
					attr = attr.values()

				values += sorted(map(str, attr))

			return hash(tuple(values))

		@property
		def epicName(self):
			if self.epic is not None:
				return self.epic.name
			return None

		@property
		def baseLabel(self):
			result = self
			while result.parent:
				result = result.parent
			return result

		@property
		def isBaseLabel(self):
			return self.parent is None

		@property
		def architectures(self):
			return self._archSet

		def restrictArchitectures(self, archSet):
			if self._archSet is None:
				return False
			self._archSet = self._archSet.intersection(archSet)
			if self.trace:
				infomsg(f"{self} restricting architectures to {self._archSet}")
			return True

		@property
		def members(self):
			if self._referencingLabels is None:
				raise Exception(f"Bad call of Label.members: {self}: self._referencingLabels not yet set")
			return self._referencingLabels

		def setDefiningBuildOption(self, optionLabel):
			assert(optionLabel.type is Classification.TYPE_BUILD_OPTION)

			if self.definingBuildOption is optionLabel:
				return

			if self.definingBuildOption is not None:
				raise Exception(f"{self}: trying to redefine in different option context: {self.definingBuildOption} vs {optionLabel}")

			self.definingBuildOption = optionLabel

			if optionLabel._referencingLabels is None:
				optionLabel._referencingLabels = Classification.createLabelSet()
			optionLabel._referencingLabels.add(self)
			# infomsg(f"{optionLabel} referenced by {self}")

		def okayToAdd(self, other):
			if self.type == other.type:
				return True

			if other.type == Classification.TYPE_CLASS:
				return self.type is Classification.TYPE_CLASS

			if self.type is Classification.TYPE_BUILD_OPTION and other.type is Classification.TYPE_EPIC:
				return True

			return False

		def addRuntimeDependency(self, other):
			if not isinstance(other, Classification.Label):
				raise Exception(f"{self}: cannot add dependency of {type(other)}: {other}")
			if not self.okayToAdd(other):
				raise Exception(f"Attempt to add incompatible dependency to {self.type} label {self}: {other} (type {other.type})")

			if self is other:
				raise Exception(f"Cannot add {other.describe()} as runtime dependency to itself")

			self.runtimeRequires.add(other)

		def configureRuntimeDependency(self, other):
			if other.type is Classification.TYPE_BUILD_OPTION:
				self.addBuildOptionDependency(other)
				return
			self.addRuntimeDependency(other)
			self.configuredRuntimeRequires.add(other)

		def safeRequirementsUpdate(self, requirements):
			if enableSafeDependencyUpdates:
				if self in requirements:
					raise Exception(f"{self}: updating requirements would create circular dependency {self} -> {self}")
				if any(self in req.runtimeRequires for req in requirements):
					culprit = None
					for req in requirements:
						if self in req.runtimeRequires:
							culprit = req
					raise Exception(f"{self}: updating requirements would create circular dependency {self} -> {culprit} -> {self}")

			self.runtimeRequires.update(requirements)

		def addBuildOptionDependency(self, other):
			assert(isinstance(other, Classification.Label))

			if other.type is not Classification.TYPE_BUILD_OPTION:
				raise Exception(f"Attempt to add incompatible option dependency to {self.type} label {self}: {other} (type {other.type})")

			self.requiredOptions.add(other)

		@property
		def hasFlavors(self):
			return bool(self._flavors)

		@property
		def flavors(self):
			return map(lambda pair: pair[1], sorted(self._flavors.items()))

		def getBuildFlavor(self, name):
			return self._flavors.get(name)

		def addBuildFlavor(self, otherLabel):
			flavorName = otherLabel.flavorName

			if self.getBuildFlavor(flavorName) is not None:
				raise Exception(f"Duplicate definition of flavor {flavorName} for {self.name}")

			self._flavors[flavorName] = otherLabel

			# flavors inherit the parent's build project by default
			if self.epic and not otherLabel.epic:
				otherLabel.setEpic(self.epic)

			if self.epic and otherLabel.epic is not self.epic:
				raise Exception(f"build flavor {otherLabel} uses source project {otherLabel.epic}, but {self} uses {self.epic}")

			assert(otherLabel.parent is self and otherLabel.flavorName == flavorName)

			# This creates a circular reference that kills garbage collection, but
			# we'll live with this for now
			#otherLabel.parent = self
			#otherLabel.flavorName = flavorName

		def setLayer(self, layerLabel):
			if layerLabel is None:
				return

			assert(self.type is Classification.TYPE_EPIC)
			assert(layerLabel.type is Classification.TYPE_LAYER)
			if self.layer is layerLabel:
				return

# We do not prevent the layer to be changed; this happens a lot because we have files where we
# set a default_layer, and then override the layer for individual epics
#			if self.layer is not None:
#				raise Exception(f"Duplicate layer for {self}: {self.layer} vs {layerLabel}")

			# If we're moving from one layer to another, remove it from the previous layer's
			# list of referecing epics
			if self.layer is not None:
				self.layer._referencingLabels.discard(self)

			self.layer = layerLabel
			layerLabel._referencingLabels.add(self)

		def setEpic(self, epicLabel):
			if self.epic is epicLabel:
				return
			if self.epic is not None:
				raise Exception(f"Duplicate source group for {self}: {self.epic} vs {epicLabel}")

			assert(isinstance(epicLabel, Classification.Label))
			assert(epicLabel.type is Classification.TYPE_EPIC)
			self.epic = epicLabel

		def __str__(self):
			return self.name

		def describe(self):
			attrs = []
			if self.type is not None:
				attrs.append(f"type={self.type}")
			if self.isIgnored:
				attrs.append("ignore")

			if not attrs:
				return self.name

			return f"{self.name} ({', '.join(attrs)})"

	# This defines a map from label type to a subclass of Classification.Label
	# For the time being, it is empty.
	labelTypeToClass = {
	}

	# rather than a regular python set, this creates a fastset that will only
	# accept members from the label domain.
	@classmethod
	def createLabelSet(klass, initialValues = None):
		return klass.domain.set(initialValues)

	@staticmethod
	def validateLabel(type, name):
		if '/' in name or '+' in name or '-' in name:
			raise Exception(f"Invalid {type} label \"{name}\": invalid characters in name")

	class LabelTracing(object):
		def __init__(self, nameMatcher):
			self.nameMatcher = nameMatcher
			self.focusLabels = Classification.createLabelSet()

		def labelCreated(self, label):
			if label.epic and label.epic.trace:
				label.trace = True

			if not self.nameMatcher.match(label.name):
				return False

			infomsg(f"Created {label.type} label {label}, tracing enable")
			self.focusLabels.add(label)
			label.trace = True
			return True

	class Scheme(object):
		def __init__(self):
			self._nextLabelId = 0
			self._final = False

			self._newLabelsAllowed = True
			self._defaultClassOrder = None
			self._defaultLayerOrder = None
			self._defaultEpicOrder = None

			self._category = {}
			for type in Classification.VALID_TYPES:
				self._category[type] = Classification.LabelCategory(type, self)

			self._cachedAllLayers = None
			self._cachedAllEpics = None
			self._cachedAllClasses = None
			self._cachedAllAutoFlavors = None
			self._cachedAllBuildOptions = None

			self.scenarioClasses = []

			self.labelTracing = None

			self.defaultArchSet = ArchSet()

			# Set this via filter.yaml to auto-assign new builds to a default epic
			self.defaultEpic = None

			self.usersClass = self.createLabel('user', Classification.TYPE_CLASS)
			self.runtimeClass = self.createLabel('runtime', Classification.TYPE_CLASS)
			self.librariesClass = self.createLabel('libraries', Classification.TYPE_CLASS)
			self.defaultClass = self.createLabel('default', Classification.TYPE_CLASS)
			self.apiClass = self.createLabel('api', Classification.TYPE_CLASS)
			self.privateClass = self.createLabel('private', Classification.TYPE_CLASS)
			self.privateApiClass = self.createLabel('private_api', Classification.TYPE_CLASS)
			self.testClass = self.createLabel('test', Classification.TYPE_CLASS)
			self.unresolvableClass = self.createLabel('unresolved', Classification.TYPE_CLASS)
			self.importantClass = self.createLabel('important', Classification.TYPE_CLASS)

			self.policy = None

		@property
		def fingerprint(self):
			values = tuple(label.fingerprint for label in self.allLabels)
			return hash(values)

		def installLabelTracing(self, matcher):
			if matcher is None:
				return
			self.labelTracing = Classification.LabelTracing(matcher)

		@property
		def defaultArchitectures(self):
			return self.defaultArchSet

		def setDefaultArchitectures(self, archSet):
			self.defaultArchSet.update(archSet)

		def getAllLabelsWithType(self, type):
			# maybe we'd get better performance if we had a copy_on_write() operator for domain.set
			return self._category[type].allLabels.copy()

		@property
		def allLayers(self):
			if self._cachedAllLayers is not None:
				return self._cachedAllLayers

			return self.getAllLabelsWithType(Classification.TYPE_LAYER)

		@property
		def allEpics(self):
			if self._cachedAllEpics is not None:
				return self._cachedAllEpics

			return self.getAllLabelsWithType(Classification.TYPE_EPIC)

		@property
		def allTopicClasses(self):
			if self._cachedAllClasses is not None:
				return self._cachedAllClasses

			return self.getAllLabelsWithType(Classification.TYPE_CLASS)

		@property
		def allAutoFlavors(self):
			if self._cachedAllAutoFlavors is not None:
				return self._cachedAllAutoFlavors

			return self.getAllLabelsWithType(Classification.TYPE_AUTOFLAVOR)

		@property
		def allBuildOptions(self):
			if self._cachedAllBuildOptions is not None:
				return self._cachedAllBuildOptions

			return self.getAllLabelsWithType(Classification.TYPE_BUILD_OPTION)

		@property
		def allEpics(self):
			if self._cachedAllEpics is not None:
				return self._cachedAllEpics

			return self.getAllLabelsWithType(Classification.TYPE_EPIC)

		def isFrozen(self, type):
			return self._category[type].frozen

		def freezeCategory(self, type):
			self._category[type].frozen = True

			if type is Classification.TYPE_EPIC:
				self._cachedAllEpics = self.allEpics
			elif type is Classification.TYPE_LAYER:
				self._cachedAllLayers = self.allLayers
			elif type is Classification.TYPE_CLASS:
				self._cachedAllClasses = self.allTopicClasses
			elif type is Classification.TYPE_AUTOFLAVOR:
				self._cachedAllAutoFlavors = self.allAutoFlavors
			elif type is Classification.TYPE_BUILD_OPTION:
				self._cachedAllBuildOptions = self.allBuildOptions
			else:
				# FIXME cache other self.all$BLAH_LABELS as well
				pass

		@property
		def isFinal(self):
			return self._final

		def getTypedLabel(self, name, labelType):
			category = self._category[labelType]
			return category.lookup(name)

		def getTypedLabelThrow(self, name, labelType):
			label = self.getTypedLabel(name, labelType)
			if label is None:
				raise Exception(f"Cannot find {labelType} label {name}: no label of this type")
			return label

		def nameToLayer(self, name):
			return self.getTypedLabelThrow(name, Classification.TYPE_LAYER)

		def nameToEpic(self, name):
			return self.getTypedLabelThrow(name, Classification.TYPE_EPIC)

		def nameToTopicClass(self, name):
			return self.getTypedLabelThrow(name, Classification.TYPE_CLASS)

		def nameToTopicClassNoThrow(self, name):
			return self.getTypedLabel(name, Classification.TYPE_CLASS)

		def nameToAutoFlavor(self, name):
			return self.getTypedLabelThrow(name, Classification.TYPE_AUTOFLAVOR)

		def nameToBuildOption(self, name):
			return self.getTypedLabelThrow(name, Classification.TYPE_BUILD_OPTION)

		def forbidNewLabels(self):
			self._newLabelsAllowed = False

		def mayCreateNewLabel(self, type, parent):
			if self.isFrozen(type):
				errormsg(f"Cannot create {type} label after this category has been declared final")
				return False

			return self._newLabelsAllowed

		def createLabel(self, name, type, parent = None):
			category = self._category[type]

			label = category.lookup(name)
			if label is None:
				if not self.mayCreateNewLabel(type, parent):
					raise Exception(f"Refusing to create new {type} label {name} in finalize()")

				label = category.createLabel(name, parent = parent)
				self._nextLabelId += 1

				label._archSet = self.defaultArchSet

				if self.labelTracing is not None:
					self.labelTracing.labelCreated(label)
			elif label.parent != parent:
				raise Exception(f"Conflicting parents for label {name}. Cannot change from {label.parent} to {parent}")
			return label

		def createFlavor(self, baseLabel, fromAutoFlavor):
			flavorName = fromAutoFlavor.name

			if baseLabel.flavorName is not None:
				raise Exception(f"Cannot derive flavor {flavorName} from label {baseLabel} because it already is a flavor")

			if baseLabel.type is Classification.TYPE_EPIC:
				label = self.createLabel(f"{baseLabel}+{flavorName}", Classification.TYPE_EXTRA, parent = baseLabel)
				label.fromAutoFlavor = fromAutoFlavor
			else:
				raise Exception(f"Cannot create flavor {flavorName} for {baseLabel.type} label {baseLabel}: unexpected type")

			label.flavorName = flavorName
			baseLabel.addBuildFlavor(label)

			return label

		def resolveLabel(self, name, type):
			if type is Classification.TYPE_LAYER:
				return self.resolveLayerLabel(name)
			if type is Classification.TYPE_EPIC:
				return self.resolveEpicLabel(name)
			if type is Classification.TYPE_AUTOFLAVOR or \
			   type is Classification.TYPE_CLASS or \
			   type is Classification.TYPE_BUILD_OPTION:
				return self.resolveOptionLabel(name, type)
			raise Exception(f"Classification.resolveLabel: unsupported label type {type}")

		def resolveBuildFlavorNew(self, label, flavorLabel):
			flavor = label.getBuildFlavor(flavorLabel.name)
			if flavor is None:
				if self._final:
					raise Exception(f"Cannot create {label}+{flavorLabel}: classification scheme already finalized")

				flavor = self.createFlavor(label, flavorLabel)
			return flavor

		def resolveLayerLabel(self, name):
			return self.createLabel(name, Classification.TYPE_LAYER)

		def resolveEpicLabel(self, name):
			assert('/' not in name)
			return self.createLabel(name, Classification.TYPE_EPIC)

		def resolveOptionLabel(self, name, type):
			Classification.validateLabel(type, name)
			return self.createLabel(name, type)

		@property
		def allLabels(self):
			allLabels = reduce(Classification.domain.set.union, (category.allLabels for category in self._category.values()))
			return sorted(allLabels, key = str)

		def addScenarioClass(self, genericScenarioClass):
			self.scenarioClasses.append(genericScenarioClass)

		@profiling
		def createOrdering(self, labelType):
			if labelType not in (Classification.TYPE_EPIC, Classification.TYPE_LAYER, Classification.TYPE_CLASS):
				raise Exception(f"Unable to create an ordering for {labelType} labels")

			relevantLabels = self.getAllLabelsWithType(labelType)

			if True:
				good = True
				for label in relevantLabels:
					if label.runtimeRequires.issubset(relevantLabels):
						continue

					for rt in label.runtimeRequires:
						if rt.type != labelType:
							infomsg(f"Error: {label} requires label {rt}, which has incompatible type {rt.type}")
							good = False

				if not good:
					raise Exception("Consistency error in label tree")

			# infomsg(f"   Building {labelType} order from {len(relevantLabels)} labels")
			order = PartialOrder(Classification.domain, f"{labelType} label order")

			for label in relevantLabels:
				order.add(label, label.runtimeRequires)

			order.finalize()

			return order

		def classOrder(self):
			if self._defaultClassOrder is not None:
				return self._defaultClassOrder

			return self.createOrdering(Classification.TYPE_CLASS)

		def layerOrder(self):
			if self._defaultLayerOrder is not None:
				return self._defaultLayerOrder

			return self.createOrdering(Classification.TYPE_LAYER)

		def epicOrder(self):
			# Until the component order has been frozen, we re-create every time
			# someone calls this function
			if self._defaultEpicOrder is not None:
				return self._defaultEpicOrder

			return self.createOrdering(Classification.TYPE_EPIC)

		def freezeClasses(self):
			self.freezeCategory(Classification.TYPE_CLASS)
			self._defaultClassOrder = self.classOrder()

		def freezeBuildOptions(self):
			self.freezeCategory(Classification.TYPE_BUILD_OPTION)

		def freezeLayers(self):
			self.freezeCategory(Classification.TYPE_LAYER)
			self._defaultLayerOrder = self.layerOrder()

		def freezeEpics(self):
			self.freezeCategory(Classification.TYPE_EPIC)
			self.autoCompleteEpicDependencies()
			self._defaultEpicOrder = self.epicOrder()

		# When we get here, the layer definitions and layer order should be frozen.
		# The set of epics should also be frozen, but the epic *order* should still be
		# modifiable
		def autoCompleteEpicDependencies(self):
			epicsForLayer = {}
			for layer in self.allLayers:
				epicsForLayer[layer] = Classification.createLabelSet()

			for epic in self.allEpics:
				if epic.layer is None:
					raise Exception(f"Epic {epic} without layer!")
				epicsForLayer[epic.layer].add(epic)

			for layer in self.allLayers:
				if epicsForLayer[layer] == layer._referencingLabels:
					continue
				delta = epicsForLayer[layer].difference(layer._referencingLabels)
				infomsg(f"{layer}: layer._referencingLabels lacks {len(delta)} labels")
				delta = layer._referencingLabels.difference(epicsForLayer[layer])
				infomsg(f"{layer}: layer._referencingLabels has {len(delta)} excess labels: {' '.join(map(str, delta))}")
				fail

			layerOrder = self.layerOrder()
			visibleEpicsForLayer = {}
			for layer in layerOrder.bottomUpTraversal():
				visible = Classification.createLabelSet()
				for lower in layerOrder.downwardClosureFor(layer):
					if lower is not layer:
						visible.update(visibleEpicsForLayer[lower])
						visible.update(lower._referencingLabels)
				visibleEpicsForLayer[layer] = visible

			messages = LocationIndexedReport()

			badEpics = []
			for epic in self.allEpics:
				layer = epic.layer

				visible = visibleEpicsForLayer[layer]

				# It's okay and expected for an epic to explicitly require other epics from the same layer.
				# It's okay (but superfluous) to require epics from lower layers; we print a message about those.
				# It is NOT okay to require epics from layers that are not visible from where we are.
				forbidden = epic.configuredRuntimeRequires.difference(layer._referencingLabels).difference(visible)
				if forbidden:
					badEpics.append((epic, forbidden))
					continue

				epic.new_requires = epic.configuredRuntimeRequires.intersection(layer._referencingLabels)
				location = epic.definingLocation

				if epic.configuredRuntimeRequires:
					if not epic.new_requires:
						messages.add(location, f"{epic}: you can drop all explicit requirements")
					else:
						excess = epic.configuredRuntimeRequires.difference(epic.new_requires)
						if excess:
							messages.add(location, f"{epic}: you can drop these requirements: {' '.join(map(str, excess))}; you only need {' '.join(map(str, epic.new_requires))}")

				epic.runtimeRequires.update(visible)
				epic.new_requires.update(visible)

			if badEpics:
				errormsg(f"{len(badEpics)} epics with invalid dependencies not covered by layer visibility rules:")
				for epic, forbidden in badEpics:
					errormsg(f"   {epic} must not require {' '.join(map(str, forbidden))}")
				raise Exception(f"Aborting")

			if messages:
				infomsg(f"NOTE: {len(messages)} epic dependencies can be cleaned up:")
				messages.render()

		def autoFlavorForBuildOptions(self, buildOptions):
			best = None
			for autoFlavor in self.allAutoFlavors:
				if not buildOptions.issubset(autoFlavor.requiredOptions):
					continue

				if best is not None:
					if len(best.requiredOptions) < len(autoFlavor.requiredOptions):
						continue

				best = autoFlavor
			return best

		@profiling
		def finalize(self):
			if self._final:
				raise Exception(f"Duplicate call to ClassificationScheme.finalize()")

			self.forbidNewLabels()

			self.freezeClasses()
			self.freezeBuildOptions()
			self.freezeLayers()
			self.freezeEpics()

			self._final = True

		def getReferencingLabels(self, target):
			return target._referencingLabels

	class LabelHints(object):
		def __init__(self, parent = None, label = None, layer = None, epic = None, buildOption = None, klass = None,
					priority = None, autoFlavor = None, options = None, scenarioBinding = None,
					potentiallyShared = True):
			self.parent = parent
			self.potentiallyShared = potentiallyShared

			self.priority = priority
			self._label = label
			self.layer = layer
			self.epic = epic
			self.autoFlavor = autoFlavor
			self.definingBuildOption = buildOption
			self.options = options or Classification.createLabelSet()
			self.klass = klass
			self.scenarioBinding = scenarioBinding
			self.lifecycleID = None

			if parent is not None:
				if self.label is None:
					self.label = parent.label

				assert(self.layer is None or self.layer is parent.layer)
				if self.layer is None:
					self.layer = parent.layer

				assert(self.epic is None or self.epic is parent.epic)
				if self.epic is None:
					self.epic = parent.epic

				if self.klass is None:
					self.klass = parent.klass
				elif self.klass is not parent.klass and parent.klass is not None:
					warnmsg(f"{self.label}: changing class to {self.klass} from {parent.klass}")

				if self.autoFlavor is None:
					self.autoFlavor = parent.autoFlavor
				elif self.autoFlavor is not parent.autoFlavor and parent.autoFlavor is not None:
					warnmsg(f"{self.label}: changing flavor to {self.autoFlavor} from {parent.autoFlavor}")

				if self.definingBuildOption is None:
					self.definingBuildOption = parent.definingBuildOption
				elif self.definingBuildOption is not parent.definingBuildOption and parent.definingBuildOption is not None:
					errormsg(f"{self.label}: change build option to {self.definingBuildOption} from {parent.definingBuildOption}")
					fail

				if self.scenarioBinding is None:
					self.scenarioBinding = parent.scenarioBinding

				if self.lifecycleID is None:
					self.lifecycleID = parent.lifecycleID

			if label is not None:
				if label.type is Classification.TYPE_LAYER:
					assert(self.layer is label)
				elif self.layer is not label.layer:
					warnmsg(f"layer mismatch for {self.label.describe()}. label layer={label.layer} hints layer={self.layer}")

				if label.type is Classification.TYPE_BUILD_OPTION:
					assert(label is self.definingBuildOption)
				else:
					assert(label.definingBuildOption is self.definingBuildOption)

			self.isPrivate = False

			# This can be set for build hints to indicate that rpms from this build
			# can be split across several epics
			self.splitOkay = False

			self.overrideArch = None
			self.excludeArch = None
			self.includeArch = None

			self.inuse = False

		def clone(self, **kwargs):
			return Classification.LabelHints(self, **kwargs)

		def unshare(self):
			if not self.potentiallyShared and not self.inuse:
				return self

			return self.clone(potentiallyShared = False)

		def getAutoFlavor(self, classificationScheme):
			if self.autoFlavor is not None:
				return self.autoFlavor
			if not self.options:
				return None
			return classificationScheme.autoFlavorForBuildOptions(self.options)

		@property
		def label(self):
			return self._label

		@label.setter
		def label(self, value):
			assert(self._label is None or self._label is value)
			self._label = value

		def overrideFlavor(self, epicFlavor):
			assert(not self.inuse)

			assert(epicFlavor.type is Classification.TYPE_EXTRA)
			assert(epicFlavor.fromAutoFlavor is not None)
			assert(not self.potentiallyShared)

			if self.epic is None:
				self.epic = epicFlavor.epic
				self.layer = epicFlavor.layer

			if epicFlavor.epic is not self.epic:
				raise Exception(f"{self}: refusing to override label with {epicFlavor} from different epic")

			self.autoFlavor = epicFlavor.fromAutoFlavor
			self._label = epicFlavor

		@property
		def isIgnored(self):
			if self.klass is None:
				return False
			return self.klass.isIgnored

		def __str__(self):
			attrs = [f"label={self.label}"]
			if self.layer is not None:
				attrs.append(f"layer={self.layer}")
			if self.epic is not None:
				attrs.append(f"epic={self.epic}")
			if self.autoFlavor is not None:
				attrs.append(f"autoFlavor={self.autoFlavor}")
			if self.definingBuildOption is not None:
				attrs.append(f"definingOption={self.definingBuildOption}")
			if self.klass is not None:
				attrs.append(f"klass={self.klass}")
			if self.lifecycleID is not None:
				attrs.append(f"lifecycle={self.lifecycleID}")
			if self.options:
				attrs.append(f"options=<{self.options}>")
			if self.scenarioBinding is not None:
				attrs.append(f"scenarioBinding={self.scenarioBinding}")
			if self.isIgnored:
				attrs.append("ignored")
			if self.potentiallyShared:
				attrs.append("shared")

			return f"LabelHints({' '.join(attrs)})"

		def updateFromMatch(self, m):
			assert(not self.potentiallyShared)

			label = m.label

			klass = m.klass
			if self.klass is None and klass is not None:
				self.klass = klass

			if m.splitOkay:
				# FIXME: check whether the pattern applies to builds
				# If not, complain (we ignore this flag for rpms)
				self.splitOkay = True

			if m.options:
				self.options = Classification.createLabelSet(m.options)
				self.autoFlavor = None

			if m.isPrivate:
				self.isPrivate = True

			if m.lifecycleID is not None:
				self.lifecycleID = m.lifecycleID

			# This needs to happen last, as the other attributes set
			# by the match are supposed to override
			if m.labelHints is not None:
				self.updateFrom(m.labelHints)


		def updateFrom(self, other):
			assert(not self.potentiallyShared)

			if self.label is None:
				self.label = other.label
			if self.layer is None:
				self.layer = other.layer
			if self.epic is None:
				self.epic = other.epic
			if self.definingBuildOption is None:
				self.definingBuildOption = other.definingBuildOption
			if self.klass is None:
				self.klass = other.klass
			if self.lifecycleID is None:
				self.lifecycleID = other.lifecycleID
			if self.autoFlavor is None:
				self.autoFlavor = other.autoFlavor

			if not self.overrideArch:
				self.overrideArch = other.overrideArch
			if not self.includeArch:
				self.includeArch = other.includeArch
			if not self.excludeArch:
				self.excludeArch = other.excludeArch

			if not self.options:
				self.options = other.options

		def reportRpmIssue(self, report, rpm, msg):
			if rpm.label is None:
				infomsg(f"{rpm}: no label")
				location = None
			else:
				location = rpm.label.definingLocation

			report.add(location, f"XXX {rpm}: {msg}")

		def checkRpmFlavor(self, rpm, report, classificationScheme):
			rpmFlavor = None

			label = rpm.label
			if label is not None:
				rpmFlavor = label.fromAutoFlavor

			hintFlavor = self.getAutoFlavor(classificationScheme)

			if rpmFlavor is not hintFlavor:
				self.reportRpmIssue(report, rpm, f"rpm flavor {rpmFlavor} vs hints {hintFlavor} because of options {self.options}")

			assert(hintFlavor is None or self.definingBuildOption is None)

		def checkRpmDefiningBuildOption(self, rpm, report):
			rpmOption = None

			label = rpm.label
			if label is not None:
				rpmOption = label.definingBuildOption

			hintOption = self.definingBuildOption

			if rpmOption is not hintOption:
				self.reportRpmIssue(report, rpm, f"rpm defining option {rpmOption} vs hints {hintOption}")

			assert(hintOption is None or self.autoFlavor is None)

	class Subset(object):
		TYPE_BUILD	= 0
		TYPE_RPM	= 1

		class MemberMatch(object):
			# type is either 0 for build patterns or 1 for rpm patterns
			def __init__(self, type, pattern, subset):
				self.exclude = False
				if pattern.startswith('!'):
					pattern = pattern[1:]
					self.exclude = True

				self.type = type
				self.subset = subset
				self.pattern = pattern
				self.priority = len(pattern)
				self.classes = None

			def addClass(self, klass):
				if self.type == 0 and self.exclude:
					raise Exception(f"Bad subset rule: exclusion pattern \"!{self.pattern}\" with class filter not allowed")
				if self.classes is None:
					self.classes = Classification.createLabelSet()
				self.classes.add(klass)

			def match(self, name):
				return fnmatch.fnmatchcase(name, self.pattern)

		def __init__(self, label):
			self.label = label
			self.buildMatches = []
			self.rpmMatches = []
			self.rpms = set()
			self.requireNames = []

			if label.type is Classification.TYPE_EPIC:
				self.epic = label
			else:
				self.epic = label.epic
			assert(self.epic is not None)

			self.trace = label.trace or self.epic.trace

		def __str__(self):
			return self.label.describe()

		# Yes, we create circular references here. Sue me.
		def addBuildMatch(self, pattern):
			match = self.MemberMatch(self.TYPE_BUILD, pattern, self)
			self.buildMatches.append(match)
			return match

		def addRpmMatch(self, pattern):
			match = self.MemberMatch(self.TYPE_RPM, pattern, self)
			self.rpmMatches.append(match)
			return match

		def addIncludes(self, name):
			self.requireNames += list(name)

		def buildClassClosure(self, classificationScheme):
			classOrder = classificationScheme.classOrder()

			for m in self.buildMatches + self.rpmMatches:
				if m.classes is not None:
					m.classes = classOrder.downwardClosureForSet(m.classes)

		def bestBuildRule(self, build):
			for m in self.buildMatches:
				if not fnmatch.fnmatchcase(build.name, m.pattern):
					continue

				if m.exclude:
					assert(not m.classes)
					return None

				return m
			return None

		def bestRpmRule(self, rpm):
			for m in self.rpmMatches:
				if not fnmatch.fnmatchcase(rpm.name, m.pattern):
					continue

				if m.classes is not None and rpm.new_class not in m.classes:
					continue

				if m.exclude:
					return None
				return m
			return None

# This can be used, for example, to map certain class labels to some other class
class LabelMapping(object):
	def __init__(self, name):
		self.name = name
		self.labelMap = {}

	def __str__(self):
		return self.name

	def defineMapping(self, srcLabel, dstLabel):
		if srcLabel in self.labelMap:
			raise Exception(f"disposition {self}: duplicate mapping for label {srcLabel}")
		self.labelMap[srcLabel] = dstLabel

	def __call__(self, klass):
		return self.labelMap.get(klass) or klass

	@classmethod
	def build(klass, name, nameToLabel, listOfPairs):
		result = klass(name)
		for srcName, dstName in listOfPairs:
			result.defineMapping(nameToLabel(srcName), nameToLabel(dstName))
		return result

class PackageLabelling(object):
	PRIORITY_DEFAULT = 5

	# A Match is associated with a pattern
	class Match(object):
		def __init__(self, pattern, type, priority, value):
			self.type = type
			self.label = None
			self.options = []
			self.klass = None
			self.splitOkay = False
			self.isPrivate = False
			self.lifecycleID = None
			self.codebaseOnly = None

			self.labelHints = None

			if self.type in ('binary', 'package', 'hints', 'role'):
				assert(isinstance(value, Classification.LabelHints))
				self.labelHints = value
				self.label = value.label
			else:
				raise Exception(f"Invalid match type {type}")

			if ' ' in pattern or '\t' in pattern:
				self.parameters = pattern.split()
				self.pattern = self.parameters.pop(0)
			else:
				self.pattern = pattern
				self.parameters = []

			# Caveat: set priority and precedence after setting the pattern
			if priority is None:
				priority = PackageLabelling.PRIORITY_DEFAULT
			self.priority = priority

		@property
		def priority(self):
			return self._priority

		@priority.setter
		def priority(self, priority):
			self._priority = priority

			assert(priority <= 10)
			precedence = (10 - priority) * 100

			# non-wildcard matches have a higher precedence than wildcarded ones
			if self.pattern and '?' not in self.pattern and '*' not in self.pattern:
				precedence += 1000

			# longer patterns have higher precedence than shorter ones
			precedence += len(self.pattern)

			self.precedence = precedence

		@property
		def value(self):
			return self.label

		@property
		def isExactMatch(self):
			return self.precedence >= 1000

		def __str__(self):
			return f"{self.pattern}[{self.value}]"

		def describe(self):
			return f"{self.type} filter pattern={self.pattern} precedence={self.precedence}: hints={self.labelHints}"

	def __init__(self):
		self.binaryMatcher = ParallelStringMatcher()
		self.buildMatcher = ParallelStringMatcher()

	def createBinaryRpmMatch(self, pattern, labelHints):
		m = self.Match(pattern, 'binary', labelHints.priority, labelHints)
		self.binaryMatcher.add(m.pattern, m)
		return m

	def createRpmHintsMatch(self, pattern, labelHints):
		m = self.Match(pattern, 'hints', labelHints.priority, labelHints)
		self.binaryMatcher.add(m.pattern, m)
		return m

	def createBuildMatch(self, pattern, labelHints):
		m = self.Match(pattern, 'package', labelHints.priority, labelHints)
		self.buildMatcher.add(m.pattern, m)
		return m

	def createRoleMatch(self, pattern, labelHints):
		m = self.Match(pattern, 'role', labelHints.priority, labelHints)
		self.binaryMatcher.add(m.pattern, m)
		return m

	def finalize(self):
		pass

	def tryToLabelPackage(self, rpm):
		if rpm.isSourcePackage:
			return None

		matches = self.binaryMatcher.match(rpm.name)

		matchFilter = None
		if rpm.new_build:
			buildHints = rpm.new_build.labelHints

			if buildHints is not None:
				if buildHints.options or buildHints.klass:
					extraHints = Classification.LabelHints(label = buildHints.epic, layer = buildHints.layer)

					# Note: priority 5 and a pattern of "" (ie length 0) result in a precedence of 500,
					# placing it *after* any other epic level rules defined by the user, but *before*
					# any class rules like "*-devel", which usually have priority 6 or higher.
					extraMatch = PackageLabelling.Match("", 'hints', 5, extraHints)

					extraHints.options = buildHints.options
					extraHints.klass = buildHints.klass
					matches.append(extraMatch)

			matchFilter = self.MatchFilter(build = rpm.new_build, codebase = self.codebaseName)

		matches = self.preprocessMatches(rpm.name, matches, rpm.trace, matchFilter)
		return self.returnMatches(rpm.name, matches, rpm.trace)

	def tryToLabelBuild(self, build):
		matches = self.buildMatcher.match(build.name)
		matches = self.preprocessMatches(build.name, matches, build.trace)
		return self.returnMatches(build.name, matches, build.trace)

	class MatchFilter(object):
		def __init__(self, build = None, codebase = None):
			self.splitOkay = False
			self.codebase = codebase

			buildHints = None
			if build is not None:
				buildHints = build.labelHints
				if buildHints is not None:
					self.splitOkay = buildHints.splitOkay

			self.buildHints = buildHints
			self.reason = ""

			self.reset()

		def reset(self):
			self.layer = None
			self.epic = None
			self.precedence = 0
			self.stop = False

			if self.buildHints and not self.splitOkay:
				self.layer = self.buildHints.layer
				self.epic = self.buildHints.epic

		def accept(self, m):
			def wildcardMatch(a, b):
				if a is None or b is None:
					return True
				return a is b

			self.reason = ""

			if m.codebaseOnly and self.codebase not in m.codebaseOnly:
				self.reason = f" IGNORED: does not apply to codebase {self.codebase}";
				return False

			if self.stop:
				self.reason = f" IGNORED: follows an exact match";
				return False

			epic = m.labelHints.epic
			if not wildcardMatch(epic, self.epic):
				self.reason = f" IGNORED: applies to epic {epic} not {self.epic}"
				return False

			layer = m.labelHints.layer
			if not wildcardMatch(layer, self.layer):
				self.reason = f" IGNORED: applies to layer {layer} not {self.layer}"
				return False

			precedence = int(m.precedence / 100)
			if precedence == self.precedence:
				self.reason = f" IGNORED: shorter match with same priority within epic {self.epic}"
				return False

			if epic is not None:
				self.epic = epic
			if layer is not None:
				self.layer = layer
			self.precedence = precedence
			if m.isExactMatch:
				self.stop = True

			return True

		def __str__(self):
			return self.reason

	def preprocessMatches(self, name, matches, trace, matchFilter = None):
		if matchFilter is None:
			matchFilter = self.MatchFilter(codebase = self.codebaseName)

		result = list(sorted(matches, key = lambda m: m.precedence, reverse = True))

		if matchFilter is not None:
			result = list(filter(matchFilter.accept, result))

		if trace:
			if not matches:
				infomsg(f"{name}: no match by filter")
			else:
				infomsg(f"{name}: matched by {len(matches)} patterns:")
				matchFilter.reset()

				for m in sorted(matches, key = lambda m: m.precedence, reverse = True):
					matchFilter.accept(m)
					infomsg(f"      {m.describe()}{matchFilter}")

		if not result:
			return None

		return result

	def returnMatches(self, name, matches, trace):
		if not matches:
			if trace:
				infomsg(f"{name}: no matches")
			return None

		if trace:
			infomsg(f"{name}: applying matches")

		labelHints = Classification.LabelHints(potentiallyShared = False)

		for m in matches:
			labelHints.updateFromMatch(m)

			if trace:
				infomsg(f"      {m.describe()}")

		if trace:
			infomsg(f"      result: {labelHints}")

		return labelHints

class LabelTreeValidator(object):
	@classmethod
	def validate(klass, classificationScheme):
		labelsMissingAnEpic = Classification.createLabelSet()

		for buildOption in classificationScheme.allBuildOptions:
			if buildOption.epic is None:
				errormsg(f"Build option {buildOption} not associated with any epic")
				labelsMissingAnEpic.add(buildOption)

			autoFlavor = classificationScheme.autoFlavorForBuildOptions(set((buildOption, )))
			if autoFlavor is None:
				warnmsg(f"You defined build option {buildOption} without corresponding autoflavor")
			elif buildOption not in autoFlavor.requiredOptions:
				warnmsg(f"autoflavor {autoFlavor} does not require {buildOption}")

		knownUndefined = Classification.createLabelSet()
		for label in classificationScheme.allEpics:
			for req in label.runtimeRequires:
				if not req.defined:
					errormsg(f"{label} requires {req}, which is not defined anywhere")
					knownUndefined.add(req)

		for label in classificationScheme.allEpics:
			if not label.defined and label not in knownUndefined:
				errormsg(f"Something instantiated component {label}, but it's not defined anywhere")
				knownUndefined.add(req)

		for label in classificationScheme.allLabels:
			for req in label.configuredRuntimeRequires:
				if not req.defined:
					req = req.baseLabel
				if not req.defined and req not in knownUndefined:
					errormsg(f"Label {label.describe()} depends on {req}, which has not been defined")
					knownUndefined.add(req)

		numUndefinedLabels = len(knownUndefined)
		numMissingEpics = len(labelsMissingAnEpic)
		if numMissingEpics or numUndefinedLabels:
			raise Exception(f"Aborting due to errors in label tree: {numMissingEpics} labels without epics; {numUndefinedLabels} undefined labels")

	@classmethod
	def displayEpics(klass, classificationScheme):
		epicOrder = classificationScheme.epicOrder()
		for epic in epicOrder.bottomUpTraversal():
			infomsg(f"{epic}:")
			for topic in sorted(epic.topicMembers, key = str):
				if topic.parent:
					continue

				infomsg(f"    {topic}")
				for flavor in topic.flavors:
					infomsg(f"    - {flavor}")

		infomsg(f"Created label tree containing {len(classificationScheme.allEpics)} epics")

class SubsetMemberResolver(object):
	def __init__(self):
		self.epicMap = {}

		self._subsets = {}
		self.rpmMap = {}
		self.builds = []

	def defineSubset(self, label):
		if label in self._subsets:
			raise Exception(f"Refusing to redefine subset {label}")

		subset = Classification.Subset(label)
		self._subsets[label] = subset

		epic = subset.epic
		if epic not in self.epicMap:
			self.epicMap[epic] = []

		self.epicMap[epic].append(subset)

		return subset

	@property
	def subsets(self):
		return self._subsets.values()

	def resolveBuild(self, build):
		def maybeUpdateRpm(rpm, m):
			have = self.rpmMap[rpm]
			if have is None or have.priority < m.priority:
				self.rpmMap[rpm] = m

		def maybeUpdateBuild(build, m):
			for rpm in build.binaries:
				if not m.classes or rpm.new_class in m.classes:
					maybeUpdateRpm(rpm, m)

		epic = build.new_epic
		if epic not in self.epicMap:
			return

		for rpm in build.binaries:
			self.rpmMap[rpm] = None
		self.builds.append(build)

		for subset in self.epicMap[epic]:
			m = subset.bestBuildRule(build)
			if m is not None:
				maybeUpdateBuild(build, m)

			for rpm in build.binaries:
				m = subset.bestRpmRule(rpm)
				if m is not None:
					assert(m.subset is subset)
					maybeUpdateRpm(rpm, m)

		# Propagate the build and rpm trace flags to the subset
		for rpm in build.binaries:
			m = self.rpmMap[rpm]
			if m is not None and (build.trace or rpm.trace):
				m.subset.trace = True

	@property
	def result(self):
		for rpm, m in self.rpmMap.items():
			if m is not None:
				yield rpm, m.subset

	def showResult(self):
		from .util import OptionalCaption

		infomsg(f"Subset membership results:")
		for build in self.builds:
			section = OptionalCaption(f"  {build}:")
			for rpm in build.binaries:
				m = self.rpmMap[rpm]
				if m is None:
					if rpm.trace:
						section(f"   - {rpm}: NO MATCH")
					continue
				section(f"   - {rpm}: {m.subset}")

class ClassificationSchemeBuilder(object):
	class LateBinding(object):
		def __init__(self, labelName, labelType, context):
			self.labelName = labelName
			self.labelType = labelType
			self.context = context

		def bind(self, schemeBuilder, defaultClassName = 'default'):
			classificationScheme = schemeBuilder.classificationScheme

			try:
				label = schemeBuilder.bindLabel(self.labelName, self.labelType)
				if label.trace or self.referencingLabel.trace:
					infomsg(f"{self.context}: {self.referencingLabel}: resolved {self} for {self.labelName}: {label}")
			except Exception as e:
				raise Exception(f"{self.context}: {self.referencingLabel}: cannot resolve {self} for {self.labelName}: {e}")

			try:
				self.apply(label, classificationScheme)
			except Exception as e:
				raise Exception(f"{self.context}: {self.referencingLabel}: cannot apply {self} for {self.labelName}: {e}")

	class LateRequiredClassBinding(LateBinding):
		def __init__(self, referencingLabel, name, context):
			super().__init__(name, Classification.TYPE_CLASS, context)
			self.referencingLabel = referencingLabel
			assert(referencingLabel.type is Classification.TYPE_CLASS)

		def apply(self, klass, classificationScheme):
			referencingLabel = self.referencingLabel
			referencingLabel.configureRuntimeDependency(klass)

		def __str__(self):
			return "class dependency"

	class LateRequiredLayerBinding(LateBinding):
		def __init__(self, referencingLabel, name, context):
			super().__init__(name, Classification.TYPE_LAYER, context)
			self.referencingLabel = referencingLabel
			assert(referencingLabel.type is Classification.TYPE_LAYER)

		def apply(self, layer, classificationScheme):
			referencingLabel = self.referencingLabel
			referencingLabel.configureRuntimeDependency(layer)

		def __str__(self):
			return "layer dependency"

	class LateRequiredEpicBinding(LateBinding):
		def __init__(self, referencingLabel, name, context):
			super().__init__(name, Classification.TYPE_EPIC, context)
			self.referencingLabel = referencingLabel

		def apply(self, epic, classificationScheme):
			referencingLabel = self.referencingLabel
			if referencingLabel.type is Classification.TYPE_EPIC:
				# Simple case: an Epic requires an Epic
				referencingLabel.configureRuntimeDependency(epic)
			elif referencingLabel.type is Classification.TYPE_BUILD_OPTION:
				# Simple as well: a build option requires an Epic
				referencingLabel.configureRuntimeDependency(epic)

		def __str__(self):
			return "epic dependency"

	class LateRequiredOptionBinding(LateBinding):
		def __init__(self, referencingLabel, name, context):
			super().__init__(name, Classification.TYPE_BUILD_OPTION, context)
			self.referencingLabel = referencingLabel

		def apply(self, label, classificationScheme):
			self.referencingLabel.addBuildOptionDependency(label)

		def __str__(self):
			return "build option dependency"

	class LateLayerBinding(LateBinding):
		def __init__(self, referencingLabel, name, context):
			super().__init__(name, Classification.TYPE_LAYER, context)
			self.referencingLabel = referencingLabel

		def apply(self, label, classificationScheme):
			self.referencingLabel.setLayer(label)

		def __str__(self):
			return "epic layer relation"

	class LateFilterRuleBinding(object):
		def __init__(self, pattern, labelHints):
			self.pattern = pattern
			self.labelHints = labelHints
			self.targetLabel = labelHints.label
			self.priority = labelHints.priority

		def bind(self, schemeBuilder):
			packageLabelling = schemeBuilder.packageLabelling
			classificationScheme = schemeBuilder.classificationScheme

			m = self.createMatch(packageLabelling)

			self.processMatchParameters(schemeBuilder, m)

			# can this ever happen?
			if m.label is None:
				return

			assert(m.label.type is Classification.TYPE_EPIC)
			assert(self.labelHints is not None)
			assert(m.label is self.labelHints.label)

			if m.options:
				optionSet = Classification.createLabelSet(m.options)
				flavor = classificationScheme.autoFlavorForBuildOptions(optionSet)

				epicFlavor = schemeBuilder.defineEpicFlavor(m.label, flavor)
				self.labelHints = self.labelHints.clone(options = optionSet, autoFlavor = flavor)

		def processMatchParameters(self, schemeBuilder, m):
			# A match may come with additional parameters, as in
			#
			#	libvamp2-sdk class=api arch-=blup
			#
			classificationScheme = schemeBuilder.classificationScheme
			originalLabel = m.label

			if m.parameters:
				classMapping = None

				m.labelHints = m.labelHints.unshare()
				labelHints = m.labelHints

				for param in m.parameters:
					if param == 'split-ok':
						m.splitOkay = True
						continue

					mapping = schemeBuilder.getClassMapping(param)
					if mapping is not None:
						if classMapping is not None:
							raise Exception(f"Conflicting class mappings {classMapping} and {mapping}")
						classMapping = mapping
						continue

					klass = classificationScheme.nameToTopicClassNoThrow(param)
					if klass is not None:
						m.klass = klass
						continue

					if '=' not in param:
						raise Exception(f"Unknown match parameter {param} for label {originalLabel}");

					(argName, argValue) = param.split('=')
					if argName == 'priority':
						m.priority = int(argValue)
					elif argName == 'class':
						m.klass = classificationScheme.nameToTopicClass(argValue)
					elif argName == 'option':
						m.options.append(classificationScheme.nameToBuildOption(argValue))
					elif argName == 'lifecycle':
						m.lifecycleID = str(argValue)
					elif argName == 'codebase':
						m.codebaseOnly = str(argValue).split(',')
					elif argName == 'arch':
						# arch=s390x,ppc64le
						labelHints.overrideArch = ArchSet(argValue.split(','))
					elif argName == 'arch+':
						# arch+=aarch64
						labelHints.includeArch = ArchSet(argValue.split(','))
					elif argName == 'arch-':
						# arch-=aarch64
						labelHints.excludeArch = ArchSet(argValue.split(','))
					else:
						raise Exception(f"Unknown match parameter {argName} in expression \"{param}\" for label {originalLabel}");

				if classMapping is not None:
					fromKlass = m.klass
					m.klass = classMapping(m.klass or classificationScheme.defaultClass)

					if classMapping.name == 'private':
						m.isPrivate = True

			return m


	class LateRpmFilterRuleBinding(LateFilterRuleBinding):
		def createMatch(self, packageLabelling):
			return packageLabelling.createBinaryRpmMatch(self.pattern, self.labelHints)

	class LateBuildFilterRuleBinding(LateFilterRuleBinding):
		def createMatch(self, packageLabelling):
			return packageLabelling.createBuildMatch(self.pattern, self.labelHints)

	class LateHintsFilterRuleBinding(LateFilterRuleBinding):
		def createMatch(self, packageLabelling):
			return packageLabelling.createRpmHintsMatch(self.pattern, self.labelHints)

	def __init__(self, scheme = None, scenarios = None):
		self.classificationScheme = scheme or Classification.Scheme()
		self.validScenarios = scenarios
		self.packageLabelling = PackageLabelling()
		self.subsetResolver = SubsetMemberResolver()
		self._lateLabelBindings = []
		self._lateFilterBindings = []

		self.policy = Policy()
		self.globalPolicySettings = self.policy.globalSettings

		self._codebase = None
		self.promises = set()
		self.scenarioBindings = {}
		self.releaseDate = {}

		# define the class mapping 'private'
		self._privateMapping = None

	def createContract(self, id):
		return self.policy.createContract(id)

	def createSupportLevel(self, *args):
		return self.policy.createSupportLevel(*args)

	def createTeam(self, id):
		return self.policy.createTeam(id)

	def getTeam(self, id):
		return self.policy.getTeam(id)

	def createLifeCycle(self, id):
		return self.policy.createLifeCycle(id)

	def getLifeCycle(self, id):
		return self.policy.getLifeCycle(id)

	def addLateRequiredClassBinding(self, klass, referencedName, context):
		self._lateLabelBindings.append(self.LateRequiredClassBinding(klass, referencedName, context))

	def addLateRequiredLayerBinding(self, layer, referencedName, context):
		self._lateLabelBindings.append(self.LateRequiredLayerBinding(layer, referencedName, context))

	def addLateRequiredEpicBinding(self, epic, referencedName, context):
		self._lateLabelBindings.append(self.LateRequiredEpicBinding(epic, referencedName, context))

	def addLateRequiredOptionBinding(self, epic, referencedName, context):
		self._lateLabelBindings.append(self.LateRequiredOptionBinding(epic, referencedName, context))

	def addLateLayerBinding(self, epic, referencedName, context):
		self._lateLabelBindings.append(self.LateLayerBinding(epic, referencedName, context))

	def addLateRpmFilterRuleBinding(self, pattern, labelHints):
		self._lateFilterBindings.append(self.LateRpmFilterRuleBinding(pattern, labelHints))

	def addLateBuildFilterRuleBinding(self, pattern, labelHints):
		self._lateFilterBindings.append(self.LateBuildFilterRuleBinding(pattern, labelHints))

	def addLateHintsFilterRuleBinding(self, pattern, labelHints):
		# for now, assume the priority is default
		assert(labelHints.priority is None)
		self._lateFilterBindings.append(self.LateHintsFilterRuleBinding(pattern, labelHints))

	def setUnresolvableClass(self, klass):
		assert(klass.type is Classification.TYPE_CLASS)

		classificationScheme = self.classificationScheme
		assert(classificationScheme.unresolvableClass in (klass, None))
		classificationScheme.unresolvableClass = klass

	@property
	def codebase(self):
		return self._codebase

	@codebase.setter
	def codebase(self, value):
		self._codebase = value
		if value is not None:
			self.packageLabelling.codebaseName = value.name
		else:
			self.packageLabelling.codebaseName = None

	@profiling
	def complete(self):
		with TimedExecutionBlock(f"finalizing classification scheme"):
			classificationScheme = self.classificationScheme
			classificationScheme.policy = self.policy

			for lateBinding in self._lateLabelBindings:
				lateBinding.bind(self)
			self._lateLabelBindings.clear()

			for lateBinding in self._lateFilterBindings:
				lateBinding.bind(self)
			self._lateFilterBindings.clear()

			self.packageLabelling.finalize()

			LabelTreeValidator.validate(classificationScheme)
			classificationScheme.finalize()

	def bindLabel(self, labelName, labelType):
		return self.classificationScheme.resolveLabel(labelName, labelType)

	def freezeCategory(self, type):
		self.classificationScheme.freezeCategory(type)


	def tryToLabelPackage(self, pkg):
		labelHints = self.packageLabelling.tryToLabelPackage(pkg)
		if labelHints is not None:
			pkg.setLabelHints(labelHints)
			debugInitialPlacement(f"{pkg} is placed in {labelHints} by package filter rules")

	def tryToLabelBuild(self, build):
		# If the build has already been labelled via 'implement_scenario', do not try
		# to update it.
		if build.labelHints and build.labelHints.scenarioBinding:
			if build.trace:
				infomsg(f"{build} was already placed in {build.labelHints} by scenario binding {build.labelHints.scenarioBinding}")
			return

		labelHints = self.packageLabelling.tryToLabelBuild(build)
		if labelHints is not None:
			build.setLabelHints(labelHints)
			debugInitialPlacement(f"{build} is placed in {labelHints} by package filter rules")

	def defineLabel(self, name, labelType, klass = None, epic = None):
		label = self.classificationScheme.resolveLabel(name, labelType)

		if klass is not None:
			if not label.tryUpdateClass(klass):
				raise Exception(f"Cannot define {label.describe()} as a {klass} class label")

		if epic is not None:
			label.setEpic(epic)
			label.defined = True

		return label

	def defineEpicFlavor(self, epic, autoFlavor):
		epicFlavor = self.classificationScheme.resolveBuildFlavorNew(epic, autoFlavor)
		assert(epicFlavor.fromAutoFlavor is autoFlavor)
		epicFlavor.defined = True
		return epicFlavor

	def defineEpicFlavorByName(self, flavorName, epic):
		assert(epic.type is Classification.TYPE_EPIC)

		buildOption = self.classificationScheme.getTypedLabel(flavorName, Classification.TYPE_BUILD_OPTION)
		if buildOption is not None:
			autoFlavor = self.classificationScheme.autoFlavorForBuildOptions(Classification.createLabelSet((buildOption, )))
		else:
			autoFlavor = self.classificationScheme.getTypedLabel(flavorName, Classification.TYPE_AUTOFLAVOR)
			if autoFlavor is None:
				autoFlavor = self.defineLabel(flavorName, Classification.TYPE_AUTOFLAVOR)

		if autoFlavor is None:
			raise Exception(f"Cannot define extra {epic}+{flavorName}: no auto-flavor found")

		return self.defineEpicFlavor(epic, autoFlavor)

	def defineLayer(self, name, **kwargs):
		return self.defineLabel(name, Classification.TYPE_LAYER, **kwargs)

	def defineEpic(self, name, **kwargs):
		return self.defineLabel(name, Classification.TYPE_EPIC, **kwargs)

	def defineOption(self, name, **kwargs):
		buildOption = self.defineLabel(name, Classification.TYPE_BUILD_OPTION, **kwargs)

		autoFlavor = self.defineLabel(name, Classification.TYPE_AUTOFLAVOR)
		autoFlavor.addBuildOptionDependency(buildOption)

		return buildOption

	def getEpic(self, name):
		return self.classificationScheme.nameToEpic(name)

	def getTopicClass(self, name):
		name = name.lstrip("_")
		return self.classificationScheme.nameToTopicClass(name)

	def getBuildOption(self, name):
		return self.classificationScheme.nameToBuildOption(name)

	def setCatchAllEpic(self, epic):
		if self.classificationScheme.defaultEpic is not None:
			raise Exception(f"Conflicting catch-all epic: {self.classificationScheme.defaultEpic} vs {epic}")
		self.classificationScheme.defaultEpic = epic

	def setEpicPolicyDefaults(self, epic, policyDefaults):
		if policyDefaults.defaultLifecycle is not None:
			epic.lifecycleID = policyDefaults.defaultLifecycle

		if policyDefaults.maintainer is not None:
			epic.maintainerID = policyDefaults.maintainer

		# same for other fields like the support policy

	def definePromise(self, name):
		if ':' in name:
			infomsg(f"filter description defines obsolete promise for {name}")
			return
		self.promises.add(name)

	_classMappingPrivate = None

	def getClassMapping(self, name):
		if name != 'private':
			return

		# For now, we support just one statically defined mapping.
		# We used to have a more complex machinery with its own syntax
		# in filter.yaml, but that is just way over the top...
		if self._classMappingPrivate is None:
			self._classMappingPrivate = LabelMapping.build('private',
				self.classificationScheme.nameToTopicClass, [
				('default',	'private'),
				('api',		'private_api'),
				('apidoc',	'private_api'),
				('test',	'private_api'),
				])

		return self._classMappingPrivate

	def implementScenario(self, epic, name, version):
		if self.validScenarios is None:
			return

		if epic in self.scenarioBindings:
			raise Exception(f"Conflicting scenarios for {epic} - should not happen")

		binding = self.validScenarios.getBinding(name, version, create = True)
		self.scenarioBindings[epic] = binding

		for build in binding.builds:
			labelHints = Classification.LabelHints(label = epic, epic = epic, layer = epic.layer, scenarioBinding = binding)
			assert(labelHints.scenarioBinding is binding)
			build.setLabelHints(labelHints)

	def defineSubset(self, label):
		return self.subsetResolver.defineSubset(label)

	class SubsetDependencyResolver(object):
		def __init__(self, subsets):
			self.byOption = {}
			self.byExtra = {}

			self.resolved = set()
			self.unresolved = set()

			for subset in subsets:
				if subset.label.type in (Classification.TYPE_BUILD_OPTION, Classification.TYPE_EXTRA):
					self.byOption[subset.label.name] = subset
				else:
					fail

				if not subset.requireNames:
					self.resolved.add(subset)
				else:
					self.unresolved.add(subset)

			# replace the name references with a reference to the real subset instance:
			for subset in self.unresolved:
				requires = set()
				for name in subset.requireNames:
					ref = self.getSubsetByName(name)
					if ref is None:
						raise Exception(f"Subset {subset} references unknown subset {name}")
					requires.add(ref)
				subset.requires = requires

		def resolveAll(self):
			while self.unresolved:
				done = set()
				for subset in self.unresolved:
					if not subset.requires.issubset(self.resolved):
						continue

					for req in subset.requires:
						subset.rpms.update(req.rpms)
					done.add(subset)

				if not done:
					return False

				self.unresolved.difference_update(done)
				self.resolved.update(done)

			return True

		def reportUnresolvables(self):
			errormsg(f"Unable to handle subsets with references (circular dependency?)")
			for subset in self.unresolved:
				infomsg(f"   {subset} requires:")
				for req in subset.requires:
					if req in self.resolved:
						infomsg(f"    - {req} (resolved)")
					else:
						infomsg(f"    - {req} (unresolved)")

			raise Exception(f"Unresolvable subsets")

		def getSubsetByName(self, name, epic = None):
			subset = self.byOption.get(name)
			if subset is None:
				subset = self.byExtra.get(f"{epic}+{name}")
			return subset

	def resolveSubsets(self, db):
		defaultClass = self.classificationScheme.defaultClass

		memberResolver = self.subsetResolver

		for subset in memberResolver.subsets:
			subset.buildClassClosure(self.classificationScheme)

		for build in db.builds:
			memberResolver.resolveBuild(build)

		# memberResolver.showResult()

		for rpm, subset in memberResolver.result:
			hints = rpm.labelHints
			if hints is not None:
				hints = hints.unshare()
				if hints.klass is None:
					hints.klass = rpm.new_class
			else:
				klass = rpm.new_class or defaultClass
				hints = Classification.LabelHints(label = klass, klass = klass, potentiallyShared = None)

			if subset.label.type is Classification.TYPE_EXTRA:
				hints.overrideFlavor(subset.label)
			elif subset.label.type is Classification.TYPE_BUILD_OPTION:
				hints.definingBuildOption = subset.label
			else:
				raise Exception(f"{subset}: bad label type in label {subset.label.describe()}")

			rpm.setLabelHints(hints)
			subset.rpms.add(rpm)

		dependencyResolver = self.SubsetDependencyResolver(memberResolver.subsets)
		dependencyResolver.resolveAll()

		if True:
			for subset in memberResolver.subsets:
				if not subset.trace:
					continue

				epic = subset.epic
				if subset.rpms:
					infomsg(f"Subset {subset} (epic {epic}) resolved to:")
					for rpm in sorted(subset.rpms, key = str):
						infomsg(f" - {rpm}; hints={rpm.labelHints}")
				else:
					errormsg(f"Subset {subset} resolves to empty set")

	# The release date is an integer in format YYYYMMDD
	def setReleaseDate(self, epic, date):
		self.releaseDate[epic] = date

	def validateLifecycleSequential(self, epic, lifecycle, policyReport):
		if lifecycle.implements is None:
			return

		if lifecycle.releaseDate is None:
			policyReport.add(f"{epic}: versioned lifecycle {lifecycle} but no release date")
			return

		today = datetime.date.today()
		valid = False

		for contract in lifecycle.contracts:
			if not contract.enabled:
				continue

			if contract.duration is None and contract.endOfSupport is None:
				policyReport.add(f"{lifecycle} is versioned but contract {contract} has no duration")
				continue

			infomsg(f"   {epic} {contract} release {lifecycle.releaseDate} end {contract.endOfSupport}")
			if lifecycle.releaseDate <= today and (contract.endOfSupport is None or today <= contract.endOfSupport):
				valid = True

		if not valid:
			infomsg(f"      {epic} outside support window")

		binding = self.scenarioBindings.get(epic)
		if binding is None:
			errormsg(f"Cannot locate binding for versioned scenario")
			return

		if binding.errors:
			infomsg(f"{epic}: scenario {binding} is not valid")
			for msg in binding.errors:
				infomsg(f"   {msg}")

			if today >= lifecycle.releaseDate:
				if valid:
					policyReport.add(f"{epic} has been released on {lifecycle.releaseDate}, but does not provide all packages for scenario {binding}")
					return

				errormsg(f"{epic} is marked for release on {lifecycle.releaseDate}, but does not provide all packages for scenario {binding}")
				valid = False

		lifecycle.scenarioBinding = binding
		lifecycle.valid = valid
		lifecycle.epic = epic

	# These may be a checks that we perform during composition
	def validateLifecycleVersioned(self, epic, lifecycle, policyReport):
		validImplementations = set()
		for lc in lifecycle.implementations:
			if lc.epic is None:
				warnmsg(f"Lifecycle {lc} does not implement any epic")
			elif lc.valid:
				validImplementations.add(lc)

		if not validImplementations:
			policyReport.add(f"{epic}: did not find any valid implementations for life cycle {lifecycle}")
			return

		maxConcurrentVersions = lifecycle.maxConcurrentVersions
		numActiveVersions = len(validImplementations)
		if maxConcurrentVersions and numActiveVersions > maxConcurrentVersions:
			# cut off the oldest implementations:
			implementations = list(reversed(sorted(validImplementations, key = lambda lc: lc.releaseDate.toordinal())))
			infomsg(f"   {epic}: life cycle {lifecycle} specifies a max of {maxConcurrentVersions} concurrent versions but we have {numActiveVersions} within the support window")

			cutoff = implementations[maxConcurrentVersions:]
			for ver in cutoff:
				infomsg(f"      cutting off old version {ver}")
			validImplementations.difference_update(set(cutoff))

		activeEpics = Classification.createLabelSet(lc.epic for lc in validImplementations)

		allEpics = Classification.createLabelSet(lc.epic for lc in validImplementations)
		inactiveEpics = allEpics.difference(activeEpics)

		missingActive = activeEpics.difference(epic.runtimeRequires)
		if missingActive:
			policyReport.add(f"{epic}: the following epics implement life cycle {lifecycle}, but are not required by {epic}: {missingActive}")

		excessInactive = inactiveEpics.intersection(epic.runtimeRequires)
		if excessInactive:
			policyReport.add(f"{epic}: requires the following inactive epics that implement {lifecycle}: {excessInactive}")

		# sort by descending release date
		for ver in reversed(sorted(validImplementations, key = lambda lc: lc.releaseDate.toordinal())):
			infomsg(f"   {epic}: valid {ver}")

		# update the versioned life cycle to reference only the valid
		# implementations
		lifecycle.implementations = validImplementations
