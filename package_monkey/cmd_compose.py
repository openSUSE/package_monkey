##################################################################
#
# product composition application
#
##################################################################
from .options import ApplicationBase
from .util import loggingFacade, debugmsg, infomsg, warnmsg, errormsg
from .util import ANSITreeFormatter
from .sick_yaml import YamlFormatter
from .compose import Composer
from .cmd_label import ClassificationGadget

class ComposerApplication(ApplicationBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def produce(self, db, **kwargs):
		gadget = ClassificationGadget(db, self.modelDescription, traceMatcher = self.traceMatcher)
		classificationResult = gadget.solve(self.productCodebase)

		composer = Composer(gadget.classificationScheme, codebaseModel=self.productCodebase, **kwargs)
		self.modelDescription.loadProductComposition(composer)

		composer.compose(classificationResult)
		return composer

	def run(self):
		db = self.loadNewDB()

		composer = self.produce(db)

		for product in composer.products:
			infomsg(f"Product {product.name}:")
			infomsg(f"   {len(product.rpms)} rpms")

		if composer.errorReport:
			infomsg(f"*** PROBLEM ***")
			infomsg(f"Encountered one or more problems while composing the products")
			composer.errorReport.display()
			if not self.opts.ignore_errors:
				errormsg(f"Please fix the above problems first")
				exit(1)

		composer.displayRpmDecisions(db)

		outputPath = self.getComposeOutputPath("output_all.yaml")
		composer.writeYamlAll(outputPath)

		outputPath = self.getComposeOutputPath("groups.yml")
		composer.writeYamlGroupsYaml(outputPath)

		outputPath = self.getComposeOutputPath("default.productcompose")
		composer.writeYamlProductComposer(outputPath, f"{self.opts.build_path}/000package-groups/default.productcompose.in")

		outputPath = self.getComposeOutputPath("components.yaml")
		composer.writeYamlComponents(outputPath)

		outputPath = self.getComposeOutputPath("lifecycle-%id.yaml")
		composer.writeYamlLifecycles(outputPath)

		outputPath = self.getComposeOutputPath("lifecycle-data-%id.txt")
		composer.writeZypperLifecycles(outputPath)

		outputPath = self.getComposeOutputPath("supportstatus-%id.txt")
		composer.writeSupportStatus(outputPath)

		outputPath = self.getComposeOutputPath("pkgmap.csv")
		composer.writePackageMap(outputPath)

class ErklaerBaerApplication(ComposerApplication):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		loggingFacade.disableTimestamps()

	def run(self):
		db = self.loadNewDB()

		composer = self.produce(db, includeExplanations = True, verbose = False)

		if not self.opts.packages:
			raise Exception(f"Missing package(s) arguments")

		for product in composer.bottomUpProductTraversal():
			infomsg(f"{product}:")

			tree = ANSITreeFormatter()
			node = tree.root

			reasoning = product.reasoning
			with loggingFacade.temporaryIndent():
				for name in self.opts.packages:
					reasoning.getJustification(name, node, verbose = False)

			for pfx, msg in tree.render():
				infomsg(f"{pfx}{msg}")
			infomsg("")
