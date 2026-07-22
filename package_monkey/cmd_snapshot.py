##################################################################
#
# Subcommand for snapshotting the results of a labelling run
# for later comparison
#
##################################################################

import sys
import os
import time
import shutil
import tempfile
import fnmatch

from .util import infomsg, errormsg, warnmsg, mergetree
from .options import ApplicationBase
from .snapshots import *

class SnapshotApplication(ApplicationBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		self.keep = False

	def run(self):
		slug = self.opts.slug
		slug = slug.lstrip('@')
		if not slug:
			errormsg(f"invalid snapshot slug {self.opts.slug}")
			return 1

		infomsg(f"Snapshotting current state as {slug}")

		snapRoot = self.getCachePath('snapshots')
		if not os.path.isdir(snapRoot):
			os.makedirs(snapRoot)

		snapFactory = SnapshotFactory(snapRoot)

		snapshot = snapFactory.createSnapshot(self.expandedStateRoot)

		if self.opts.with_rpms:
			snapshot.addRpms(self.productCodebase, self.getCachePath("rpmhdrs"))

		if not self.keep:
			snapFactory.remove(slug)

		snapFactory.remember(slug, snapshot)

class PublishApplication(ApplicationBase):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	def run(self):
		path = self.opts.path
		if not os.path.isdir(path):
			raise Exception(f"Cannot publish to {path}: not a directory")

		data = self.load(self.opts.slug)

		if self.opts.slug:
			source = f"snapshot {self.opts.slug}"
		else:
			source = f"current state"

		scope = self.opts.scope or 'all'
		if scope == 'all':
			infomsg(f"Publishing {source} to {path}")
			self.copyFiles(data.path, path)
		elif scope == 'lifecycle':
			infomsg(f"Publishing lifecycle from {source} to {path}")
			self.publishLifecycle(data, path)
		elif scope == 'compose':
			infomsg(f"Publishing compose files from {source} to {path}")
			self.publishCompose(data, path)
		else:
			raise Exception(f"Cannot publish {scope}: not implemented")

		return 0

	def load(self, slug):
		if slug is None:
                        return self.data

		data = self.getSnapshot(slug)
		if data is None:
			raise Exception(f"Unknown snapshot {slug}")

		return data

	def publishCompose(self, data, destDir):
		release = self.modelDescription.releaseID

		productData = data.getProduct(release)
		sourceDir = productData.path

		self.copyFiles(sourceDir, destDir,
					onlyPatterns = ("default.productcompose",))

	def publishLifecycle(self, data, destDir):
		release = self.modelDescription.releaseID

		productData = data.getProduct(release)
		sourceDir = productData.path

		targetName = f"lifecycle-data-{release}"
		archiveName = f"{targetName}.tar.xz"

		with tempfile.TemporaryDirectory() as tmpDir:
			infomsg(f"Copying lifecycle data from {sourceDir} to {tmpDir}")

			self.copyFiles(sourceDir, f"{tmpDir}/{targetName}",
						onlyPatterns = ("lifecycle*.txt", "lifecycle*.yaml"))
			os.system(f"tar -C {tmpDir} -cvjf {archiveName} {targetName}")

			shutil.move(f"{archiveName}", f"{destDir}/{archiveName}")

	def copyFiles(self, sourceDir, destDir, onlyPatterns = None):
		if onlyPatterns is None:
			mergetree(sourceDir, destDir, dirs_exist_ok = True)
		else:
			def ignoreNames(dir, entries):
				result = []
				for name in entries:
					if not any(fnmatch.fnmatch(name, pattern) for pattern in onlyPatterns):
						result.append(name)
				return result

			mergetree(sourceDir, destDir, dirs_exist_ok = True, ignore = ignoreNames)
