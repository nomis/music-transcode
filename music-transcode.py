#!/usr/bin/env python3
# music-transcode - Convert music from FLAC to a lower bitrate format
# Copyright 2020  Simon Arlott
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import grp
import logging
import multiprocessing as mp
import mutagen.flac
import mutagen.oggvorbis
import os
import pwd
import shutil
import stat
import subprocess
import sys


extensions = set([
	"flac",
	"mp3",
	"ogg",
	"m4a",
])
format = "ogg"
extra = set([
	"cover.jpg",
])

root = logging.getLogger()
root.setLevel(level=logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(processName)-10s %(levelname)-8s %(message)s"))
root.addHandler(handler)


def filter_file(name):
	if name.split(".")[-1] in extensions:
		return True
	if name in extra:
		return True
	return False


def copy_file(args):
	(name, src, dst) = args
	logging.info(f"Create {name}")
	subprocess.run(["cp", "--reflink=auto", "--no-preserve=mode,ownership,timestamps", "--", os.path.join(src, name).encode("utf8", "surrogateescape"), os.path.join(dst, name).encode("utf8", "surrogateescape")], check=True)

def copy_flac(args):
	(name, src, dst) = args
	logging.info(f"Convert {name}.flac to {name}.{format}")
	subprocess.run(["oggenc", "--quality", "6", "--discard-comments", "--quiet", b"--output=" + os.path.join(dst, f"{name}.ogg~").encode("utf8", "surrogateescape"), "--", os.path.join(src, f"{name}.flac").encode("utf8", "surrogateescape")], check=True)
	os.rename(os.path.join(dst, f"{name}.ogg~"), os.path.join(dst, f"{name}.ogg"))
	sync_flac(args)


def sync_flac(args):
	(name, src, dst) = args
	src = mutagen.flac.FLAC(os.path.join(src, f"{name}.flac"))
	dst = mutagen.oggvorbis.OggVorbis(os.path.join(dst, f"{name}.ogg"))
	if sorted(src.tags) != sorted(dst.tags):
		logging.debug(f"Tag {name}.{format}")
		dst.tags.clear()
		dst.tags.extend(src.tags)
		dst.save()


def sync_paths(src, dst, user=None):
	access = {}
	pwnam = pwd.getpwnam(user)
	uid = pwnam.pw_uid
	groups = set([pwnam.pw_gid] + [g.gr_gid for g in grp.getgrall() if pwnam.pw_name in g.gr_mem])

	def _has_access(path, target=False):
		if path in access:
			return access[(path, target)]

		parent = os.path.dirname(path)
		if parent != path:
			if not _has_access(parent, target):
				access[(path, target)] = False
				return False

		try:
			st = os.lstat(path)
		except FileNotFoundError:
			access[(path, target)] = False
			return False

		if stat.S_ISLNK(st.st_mode):
			access[(path, target)] = _has_access(os.path.join(os.path.dirname(path), os.readlink(path)), True)
		elif stat.S_ISDIR(st.st_mode):
			if st.st_uid == uid:
				if target or len(path) <= len(src):
					access[(path, target)] = (st.st_mode & stat.S_IXUSR) != 0
				else:
					access[(path, target)] = (st.st_mode & (stat.S_IRUSR | stat.S_IXUSR)) == (stat.S_IRUSR | stat.S_IXUSR)
			elif st.st_gid in groups:
				if target or len(path) <= len(src):
					access[(path, target)] = (st.st_mode & stat.S_IXGRP) != 0
				else:
					access[(path, target)] = (st.st_mode & (stat.S_IRGRP | stat.S_IXGRP)) == (stat.S_IRGRP | stat.S_IXGRP)
			else:
				if target or len(path) <= len(src):
					access[(path, target)] = (st.st_mode & stat.S_IXOTH) != 0
				else:
					access[(path, target)] = (st.st_mode & (stat.S_IROTH | stat.S_IXOTH)) == (stat.S_IROTH | stat.S_IXOTH)
		else:
			if st.st_uid == uid:
				access[(path, target)] = (st.st_mode & stat.S_IRUSR) != 0
			elif st.st_gid in groups:
				access[(path, target)] = (st.st_mode & stat.S_IRGRP) != 0
			else:
				access[(path, target)] = (st.st_mode & stat.S_IROTH) != 0
		return access[(path, target)]

	src_dirs = set()
	src_files = set()
	dst_dirs = set()
	dst_files = set()

	for root, dirs, files in os.walk(src):
		for name in dirs:
			name = os.path.join(root, name)
			if _has_access(name):
				src_dirs.add(os.path.relpath(name, src))
		for name in files:
			name = os.path.join(root, name)
			if filter_file(name) and _has_access(name):
				src_files.add(os.path.relpath(name, src))

	for root, dirs, files in os.walk(dst):
		for name in dirs:
			name = os.path.join(root, name)
			dst_dirs.add(os.path.relpath(name, dst))
		for name in files:
			name = os.path.join(root, name)
			dst_files.add(os.path.relpath(name, dst))

	src_flac_files = set()
	src_as_format_files = set()
	src_not_flac_files = set()
	for name in src_files:
		parts = name.split(".")
		if parts[-1] == "flac":
			name = ".".join(parts[0:-1])
			src_flac_files.add(name)
			src_as_format_files.add(f"{name}.{format}")
		else:
			src_not_flac_files.add(name)
			src_as_format_files.add(name)

	dst_format_files = set()
	for name in dst_files:
		parts = name.split(".")
		if parts[-1] == format:
			name = ".".join(parts[0:-1])
			dst_format_files.add(name)

	for name in src_flac_files & dst_format_files:
		if os.lstat(os.path.join(dst, f"{name}.{format}")).st_mtime < os.lstat(os.path.join(src, f"{name}.flac")).st_mtime:
			logging.debug(f"Refresh {name}.flac")
			os.unlink(os.path.join(dst, f"{name}.{format}"))
			dst_format_files.remove(name)
			dst_files.remove(f"{name}.{format}")

	for name in src_not_flac_files & dst_files:
		if os.lstat(os.path.join(dst, name)).st_mtime < os.lstat(os.path.join(src, name)).st_mtime:
			logging.debug(f"Refresh {name}")
			os.unlink(os.path.join(dst, name))
			dst_files.remove(name)

	for name in dst_files - src_as_format_files:
		logging.info(f"Delete {name}")
		os.unlink(os.path.join(dst, name))

	for name in dst_dirs - src_dirs:
		logging.info(f"Delete {name}")
		shutil.rmtree(os.path.join(dst, name), ignore_errors=True)

	for name in src_dirs - dst_dirs:
		logging.debug(f"Create {name}")
		os.makedirs(os.path.join(dst, name), exist_ok=True)

	with mp.Pool(os.cpu_count()) as p:
		p.map(copy_file, [(name, src, dst) for name in (src_not_flac_files - dst_files)])
		p.map(copy_flac, [(name, src, dst) for name in (src_flac_files - dst_format_files)])
		p.map(sync_flac, [(name, src, dst) for name in (src_flac_files & dst_format_files)])


if __name__ == "__main__":
	mp.current_process().name = "Main"

	parser = argparse.ArgumentParser(description="Convert music from FLAC to a lower bitrate format")
	parser.add_argument("--src", metavar="PATH", type=str, required=True, help="Source path")
	parser.add_argument("--dst", metavar="PATH", type=str, required=True, help="Destination path")
	parser.add_argument("--user", metavar="USER", type=str, help="Ignore source files that are not accessible by USER")

	args = parser.parse_args()
	logging.debug("start")
	sync_paths(args.src, args.dst, args.user)
	logging.debug("stop")
