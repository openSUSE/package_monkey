import profile
import argparse

from .products import ProductCodebase, CacheLocation
from .util import debugmsg, infomsg, warnmsg, errormsg
from .util import NameMatcher
from .obsclnt import OBSClient
from .compose import Composer
from .filter import Classification
from .model import ComponentModelMapping
from .usecase import UseCaseCatalog
from .preprocess import *
from .snapshots import *

import os

class ApplicationBase(object):
	def __init__(self, name, opts = None):
		self.name = name
		self._opts = opts
		self.opts = opts

		self._stateRoot = None
		self._cache = None

		self._modelDescription = None
		self._data = None
		self._codebaseData = None
		self._productData = None
		self._snapshots = None

	@property
	def modelDescription(self):
		if self._modelDescription is None:
			self._modelDescription = ModelDescription(self.opts)
			if self._modelDescription._releaseID is None:
				self._modelDescription._releaseID = self.productCodebase.release
		return self._modelDescription

	def getModelPath(self, basename):
		return self.modelDescription.getPath(basename)

	# FIXME: rename to codebaseModel
	@property
	def productCodebase(self):
		return self.modelDescription.codebaseModel

	@property
	def productRelease(self):
		release = getattr(self.opts, 'release')
		if release is None:
			# default to what the codebase specifies as default
			release = self.productCodebase.release
		assert(release is not None)
		return release

	@property
	def expandedStateRoot(self):
		if self._stateRoot is None:
			self._stateRoot = os.path.expanduser(self.opts.statedir)
		return self._stateRoot

	@property
	def data(self):
		if self._data is None:
			self._data = Snapshot(self.expandedStateRoot)
		return self._data

	def getSnapshot(self, slug):
		if slug is None or slug == '@@':
			return self.data

		if self._snapshots is None:
			snapRoot = self.getCachePath('snapshots')
			self._snapshots = SnapshotFactory(snapRoot)

		return self._snapshots.load(slug.lstrip('@'))

	def getCodebaseForSnapshot(self, slug = None):
		data = self.getSnapshot(slug)
		if data is None:
			raise Exception(f"Cannot locate snapshot {slug}")
		return data.getCodebase(self.opts.codebase)

	@property
	def codebaseData(self):
		if self._codebaseData is None:
			self._codebaseData = self.data.getCodebase(self.opts.codebase)
		return self._codebaseData

	def getCodebasePath(self, basename):
		return self.codebaseData.getPath(basename)

	@property
	def productData(self):
		if self._productData is None:
			self._productData = self.data.getProduct(self.productRelease)
		return self._productData

	def getComposeOutputPath(self, basename):
		return self.productData.getPath(basename)

	@property
	def cacheRootPath(self):
		return os.path.expanduser(self.opts.cache)

	@property
	def cache(self):
		if self._cache is None:
			self._cache = CacheLocation(self.opts.cache)
		return self._cache

	def loadDBForSnapshot(self, slug = None):
		codebaseData = self.getCodebaseForSnapshot(slug)
		return codebaseData.loadDB(traceMatcher = self.traceMatcher)

	def loadNewDB(self, **kwargs):
		return self.codebaseData.loadDB(traceMatcher = self.traceMatcher, **kwargs)

	def saveDB(self, db):
		return self.codebaseData.saveDB(db)

	def savePolicy(self, classificationScheme):
		self.codebaseData.savePolicy(classificationScheme)

	def loadPolicy(self, labelFacade):
		return self.codebaseData.loadPolicy(labelFacade)

	def loadClassificationForSnapshot(self, slug = None):
		codebaseData = self.getCodebaseForSnapshot(slug)
		return codebaseData.loadClassification()

	@property
	def traceMatcher(self):
		if not self.opts.trace:
			return None

		names = []
		for arg in self.opts.trace:
			names += arg.split(',')

		return NameMatcher(names)

	@property
	def defaultHttpPath(self):
		return self.getCachePath("http")

	def getCachePath(self, subdir):
		return f"{self.cacheRootPath}/{subdir}"

class ModelDescription(object):
	def __init__(self, opts):
		path = opts.model_path
		if path is None:
			path = os.getenv("MONKEY_MODEL_PATH")
		if path is None:
			path = "."
		self.path = path

		self._codebaseID = opts.codebase
		self._codebaseExtraBuildProjects = opts.extra_build_project
		# Not all commands support the --release option
		self._releaseID = getattr(opts, 'release', None)

		self._codebaseModel = None

	@property
	def releaseID(self):
		assert(self._releaseID is not None)
		return self._releaseID

	def getPath(self, *args):
		return os.path.join(self.path, *args)

	@property
	def codebaseModel(self):
		if self._codebaseModel is None:
			name = self._codebaseID
			if name is None:
				raise Exception("Cannot determine codebase, please specify --codebase option")

			codebase = ProductCodebase.load(name, self.getPath(f"{name}.yaml"))
			for project in self._codebaseExtraBuildProjects:
				codebase.buildProjects.append(project)
			self._codebaseModel = codebase

		return self._codebaseModel

	def loadPreprocessorHints(self):
		codebase = self.codebaseModel
		if codebase.hintsFile is not None:
			path = self.getPath(codebase.hintsFile)
			if not os.path.isfile(path):
				raise Exception(f"Cannot access hints file at {path}")
		else:
			path = self.getPath('hints.conf')
		hintsLoader = PreprocessorHintsLoader(path)
		return hintsLoader.load()

	def loadProductComposition(self, composer):
		if self._releaseID is None:
			raise Exception("Cannot determine product release, please specify --release option")

		path = self.getPath(self._releaseID, 'compose.yaml')

		infomsg(f"Reading product composition from {path}")
		composer.loadProductComposition(path)

		useCaseCatalog = self.loadUsecaseCatalog()
		if useCaseCatalog is not None:
			composer.useCaseCatalog = useCaseCatalog

		composer.signoffs = self.loadSignoffs()

	def loadUsecaseCatalog(self):
		if self._releaseID is None:
			raise Exception("Cannot determine product release, please specify --release option")

		path = self.getPath(self._releaseID, 'usecase.yaml')
		if not os.path.exists(path):
			return None

		infomsg(f"Reading use case catalog from {path}")
		return UseCaseCatalog.loadFromYaml(path)

	# This should live elsewhere:
	class Signoff(object):
		def __init__(self, release, epic, owner, hash, date = None):
			self.release = release
			self.owner = owner
			self.epic = epic
			self.hash = hash
			self.date = date

	class Notary(object):
		def __init__(self):
			self.signoffs = {}

		def addSignoff(self, s):
			self.signoffs[s.epic] = s

		def recordSignoff(self, *args, **kwargs):
			signoff = ModelDescription.Signoff(*args, **kwargs)
			self.addSignoff(signoff)

		def lookupEpic(self, epic):
			epic = str(epic)
			return self.signoffs.get(epic)

	def addSignoffs(self, signoffs):
		path = self.getPath(self._releaseID, 'signoffs.txt')
		with open(path, "a") as f:
			for s in signoffs:
				print(f"Signoff {s.date} {s.hash} {s.release} {s.epic} {s.owner}", file = f)

		infomsg(f"Added {len(signoffs)} signoffs to {path}")

	def loadSignoffs(self):
		notary = self.Notary()

		path = self.getPath(self._releaseID, 'signoffs.txt')
		if not os.path.exists(path):
			return notary

		with open(path, "r") as f:
			for line in f.readlines():
				line = line.strip()
				if not line:
					continue

				cmd, rest = line.split(maxsplit = 1)
				if cmd.lower() != 'signoff':
					raise Exception("{path}: cannot handle unknown command in line {line}")

				date, hash, release, epic, owner = rest.split(maxsplit = 4)
				notary.recordSignoff(release, epic, owner, hash, date)

		return notary
