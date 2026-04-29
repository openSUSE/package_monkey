from .options import ApplicationBase
from .util import loggingFacade, debugmsg, infomsg, warnmsg, errormsg
from .util import NameMatcher
from .arch import *
from .postprocess import *

loggingFacade.disableTimestamps()

class CommonInfoApplication(ApplicationBase):
	def __init__(self, name, opts):
		super().__init__(name, opts)

		self.db = None
		self.extraDB = None

	def run(self):
		if (self.opts.requires_only + self.opts.provides_only + self.opts.names_only) > 1:
			errormsg(f"You can specify only one of --requires-only --provides-only or --names-only")
			exit(1)

		codebaseData = self.getCodebaseForSnapshot(None)
		self.db = codebaseData.loadDB()

		self.db.enableProvidesLookups()

		self.renderer = Renderer(self.opts, codebaseData)
		self.processQuery(self.db, self.opts.packages)

	def renderOneRpm(self, rpm, obsBuild = None):
		renderer = self.renderer

		print(f"{renderer.renderRpmName(rpm)}")

		renderer.renderRpmInfo(rpm)
		renderer.renderPolicy(rpm)
		renderer.renderScenarios(rpm)

		if obsBuild is not None:
			renderer.renderBuildInfo(obsBuild, exceptRpm = rpm)

		renderer.renderRequires(rpm)
		renderer.renderUnresolvables(rpm)

		if not rpm.isSourcePackage:
			renderer.renderProvides(rpm)

class PackageInfoApplication(CommonInfoApplication):
	def processQuery(self, db, nameList):
		validArchitectures = ('src', 'nosrc', 'noarch', 'i686', 'x86_64', 'aarch64', 's390x', 'ppc64le', )
		for packageName in nameList:
			packageArch = None
			if '.' in packageName:
				baseName, arch = packageName.rsplit('.', maxsplit = 1)
				if arch in validArchitectures:
					packageName, packageArch = baseName, arch

			matcher = NameMatcher([packageName])
			rpmList = []
			for rpm in db.rpms:
				if matcher.match(rpm.name):
					rpmList.append(rpm)

			if not rpmList:
				print(f"{packageName}: no match")
				continue

			for rpm in rpmList:
				self.renderOneRpm(rpm, rpm.new_build)

class BuildInfoApplication(CommonInfoApplication):
	def processQuery(self, db, nameList):
		for buildName in nameList:
			matcher = NameMatcher([buildName])
			buildList = []
			for build in db.builds:
				if matcher.match(build.name):
					buildList.append(build)

			if not buildList:
				print(f"{buildName}: not found")
				continue

			for build in buildList:
				if not build.rpms:
					print(f"{build}: no rpms for this package?!")
					continue

				print(f"Build {build} ({len(build.binaries)} rpms)")
				for rpm in build.binaries:
					self.renderOneRpm(rpm)
				print("")

class TrivialStringRenderer(object):
	def render(self, s):
		return str(s)

class RpmNameRenderer(object):
	def __init__(self, labelFacade = None):
		self.labelFacade = labelFacade

	def render(self, rpm, arch = None):
		result = rpm.name

		if rpm.isExternal:
			result += " [GHOST]"

		if self.labelFacade is not None:
			labelHints = self.labelFacade.getHintsForRpm(rpm)
			if labelHints is not None:
				result += f" ({labelHints})"
			if rpm.type == rpm.TYPE_MISSING:
				result += " [MISSING]"

		if arch is not None:
			result += f" [{arch}]"
		return result

class PolicyRenderer(object):
	def __init__(self, labelFacade):
		self.labelFacade = labelFacade
		self.policy = labelFacade.policy

	def render(self, rpm):
		if self.policy is None:
			return

		epic = self.labelFacade.getEpicForBuild(rpm.new_build)
		if epic is None:
			return

		if epic.ownerID:
			team = self.policy.getTeam(epic.ownerID)
			if team is not None:
				print(f"  reviewer: {team}")
			else:
				print(f"  reviewer: {epic.ownerID}")

		if epic.lifecycleID:
			lifecycle = self.policy.getLifeCycle(epic.lifecycleID)
			if lifecycle is not None:
				print(f"  lifecycle: {lifecycle}")
			else:
				print(f"  lifecycle: {epic.lifecycleID}")

class ListRenderer(object):
	def __init__(self, itemRenderer):
		self.itemRenderer = itemRenderer
		self.count = None

	def __del__(self):
		self.endList()

	def beginList(self):
		if self.count is None:
			self.count = 0

	def endList(self):
		if self.count == 0 and self.MSG_EMPTY is not None:
			print(f"  {self.MSG_EMPTY}")
		self.count = None

	def renderItem(self, *args, renderFunc = None, **kwargs):
		if self.count is None:
			raise Exception(f"You must call ListRenderer.beginList() first")

		if self.count == 0:
			print(f"  {self.MSG_HEADER}:")
		self.count += 1

		if renderFunc is None:
			renderFunc = self.itemRenderer.render

		item = renderFunc(*args, **kwargs)
		print(f"    - {item}")

class DependencyRenderer(ListRenderer):
	def renderPackageList(self, packages, **kwargs):
		self.beginList()
		for rpm in sorted(packages, key = str):
			self.renderItem(rpm, **kwargs)

class RequiresRenderer(DependencyRenderer):
	MSG_EMPTY = "does not require anything"
	MSG_HEADER = "requires"

	def render(self, rpm):
		labelHints = rpm.labelHints
		labelHints = self.itemRenderer.labelFacade.getHintsForRpm(rpm)
		if labelHints is not None and labelHints.requiredOptions:
			print("  depends on build option(s):")
			for option in sorted(labelHints.requiredOptions):
				print(f"    - {option}")

		self.beginList()

		common = rpm.solutions.common
		self.renderPackageList(common)
		self.renderPackageList(rpm.conditionals.common, renderFunc = self.renderConditional)

		specific = {}
		for arch in rpm.architectures:
			for req in rpm.getDependencies(arch).difference(common):
				if req not in specific:
					specific[req] = ArchSet()
				specific[req].add(arch)
			for req in rpm.getConditionals(arch).difference(rpm.conditionals.common):
				if req not in specific:
					specific[req] = ArchSet()
				specific[req].add(arch)

		for req in sorted(specific.keys(), key = str):
			if type(req) is not str:
				self.renderItem(req, arch = specific[req])
			else:
				self.renderItem(req, arch = specific[req], renderFunc = self.renderConditional)

		self.endList()

	def renderConditional(self, name, arch = None):
		result = f"conditional: {name}"
		if arch is not None:
			result += f" [{arch}]"
		return result

class ProvidesRenderer(DependencyRenderer):
	MSG_EMPTY = "not required by anything"
	MSG_HEADER = "required by"

	def __init__(self, db, rpmNameRenderer):
		super().__init__(rpmNameRenderer)
		self.db = db

	def render(self, rpm):
		self.renderPackageList(rpm.requiredBy.common)

		specific = {}
		for arch in rpm.architectures:
			for prov in rpm.requiredBy.raw_get(arch).difference(rpm.requiredBy.common):
				if prov not in specific:
					specific[prov] = ArchSet()
				specific[prov].add(arch)

		for prov in sorted(specific.keys(), key = str):
			self.renderItem(prov, arch = specific[prov])

		self.endList()

class UnresolvableRenderer(ListRenderer):
	MSG_EMPTY = None
	MSG_HEADER = "unresolvable requires"

	def __init__(self):
		super().__init__(TrivialStringRenderer())

	def render(self, rpm):
		self.beginList()

		for dep in sorted(rpm.enumerateUnresolvedDependencies(), key = str):
			self.renderItem(dep)

		self.endList()

class RpmSummaryRenderer(object):
	def __init__(self, db):
		self.db = db

	def render(self, rpm):
		self.renderBasicInfo(rpm)

		auxInfo = self.db.lookupRpm(rpm.name, 'x86_64')
		if auxInfo is not None:
			self.renderAuxInfo(rpm, auxInfo)

	def renderBasicInfo(self, rpm):
		vlist = list(rpm.versions.common)
		if len(vlist) == 1:
			print(f"  version: {vlist[0]}")

		print(f"  architectures: {rpm.architectures or 'none'}")


	def renderAuxInfo(self, rpm, auxInfo):
		if auxInfo.summary is not None:
			print(f"  summary: {auxInfo.summary}")

class RpmSummaryRendererLong(RpmSummaryRenderer):
	def renderAuxInfo(self, rpm, auxInfo):
		super().renderAuxInfo(rpm, auxInfo)
		if auxInfo.description is not None:
			lines = auxInfo.description.split('\n')
			if len(lines) == 1:
				print(f"  description: {lines[0]}")
			else:
				print(f"  description:")
				for line in lines:
					print(f"            {line}")

class OBSBuildNameRenderer(object):
	def render(self, obsBuild, renderer, exceptRpm = None):
		print(f"  OBS build: {obsBuild}")

class OBSBuildSiblingRenderer(OBSBuildNameRenderer):
	def render(self, obsBuild, renderer, exceptRpm = None):
		super().render(obsBuild, renderer)

		src = obsBuild.sourceRpm

		headerShown = False
		for sib in obsBuild.binaries:
			if sib is exceptRpm or sib is src:
				continue

			if not headerShown:
				print(f"     Sibling packages:")
				headerShown = True

			print(f"        {renderer.renderRpmName(sib)}")

class Renderer(object):
	def __init__(self, opts, codebaseData):
		db = codebaseData.loadDB()

		labelFacade = None
		if not opts.no_labels:
			labelFacade = codebaseData.loadClassification()

		with_rpmInfo = True
		with_provides = True
		with_requires = True
		with_buildInfo = True
		with_policy = True

		if opts.requires_only:
			with_provides = False
			with_policy = False
		if opts.provides_only:
			with_requires = False
			with_policy = False
		if opts.names_only:
			with_rpmInfo = False
			with_provides = False
			with_requires = False
			with_buildInfo = False
			with_policy = False

		self.rpmPackageInfo = None
		self.obsPackageInfo = None
		self.requires = None
		self.provides = None
		self.unresolvable = None
		self.policy = None

		self.rpm = RpmNameRenderer(labelFacade)

		if with_rpmInfo:
			extraDB = codebaseData.loadExtraDB()
			if opts.verbose:
				self.rpmPackageInfo = RpmSummaryRendererLong(extraDB)
			else:
				self.rpmPackageInfo = RpmSummaryRenderer(extraDB)

		if with_buildInfo:
			if opts.siblings:
				self.obsPackageInfo = OBSBuildSiblingRenderer()
			else:
				self.obsPackageInfo = OBSBuildNameRenderer()

		if with_requires:
			self.requires = RequiresRenderer(self.rpm)
			self.unresolvable = UnresolvableRenderer()
		if with_provides:
			self.provides = ProvidesRenderer(db, self.rpm)
		if with_policy and labelFacade is not None:
			self.policy = PolicyRenderer(labelFacade)

	def renderBuildInfo(self, obsBuild, **kwargs):
		if self.obsPackageInfo is not None:
			self.obsPackageInfo.render(obsBuild, self, **kwargs)

	def renderRpmName(self, rpm):
		return self.rpm.render(rpm)

	def renderPolicy(self, rpm):
		if self.policy is not None:
			self.policy.render(rpm)

	def renderScenarios(self, rpm):
		scenarios = rpm.validForScenarios
		if scenarios:
			print(f"  valid scenarios: {' '.join(sorted(map(str, scenarios)))}")

	def renderRpmInfo(self, rpm):
		if self.rpmPackageInfo is not None:
			self.rpmPackageInfo.render(rpm)

	def renderRequires(self, rpm):
		if self.requires is not None:
			self.requires.render(rpm)

	def renderUnresolvables(self, rpm):
		if self.unresolvable is not None:
			self.unresolvable.render(rpm)

	def renderProvides(self, rpm):
		if self.provides is not None:
			self.provides.render(rpm)
