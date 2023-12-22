#!/usr/bin/env python3
# music-transcode - Convert music from FLAC to a lower bitrate format
# Copyright 2020-2023  Simon Arlott
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
import math
import multiprocessing as mp
import mutagen.easyid3
import mutagen.flac
import mutagen.mp3
import mutagen.oggvorbis
import os
import pwd
import re
import shutil
import stat
import subprocess
import sys
import unidecode


extensions = set([
	"flac",
	"mp3",
	"ogg",
	"m4a",
])
extra = set([
	"cover.jpg",
	"playlist-christmas.jpg",
])
re_unsafe_d = re.compile(r"[^A-Za-z0-9 .,&'()_/-]")
re_unsafe_f = re.compile(r"[^A-Za-z0-9 .,&'()_-]")
re_unsafe_android = re.compile(r"[\"*:<>?\\|]")

root = logging.getLogger()
root.setLevel(level=logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(processName)-10s %(levelname)-8s %(message)s"))
root.addHandler(handler)


def filter_file(name, no_extra):
	name = os.path.basename(name)
	if name.split(".")[-1] in extensions:
		return True
	if not no_extra and name in extra:
		return True
	return False


def round_mtime(filename):
	stat = os.lstat(filename)
	if stat.st_mtime % 2 != 0:
		mtime = (math.ceil(stat.st_mtime) + 1) // 2 * 2
		os.utime(filename, times=(stat.st_atime, mtime))


def safe_filename(name):
	if name.startswith("/") or name.startswith("./") or name.startswith("../"):
		return False
	if name.endswith("/") or name.endswith("/.") or name.endswith("/.."):
		return False
	if "/./" in name or "/../" in name:
		return False
	return True


def copy_file(args):
	(src, src_name, dst, dst_name, fat) = args
	logging.info(f"Create {dst_name}")
	assert safe_filename(src_name), src_name
	assert safe_filename(dst_name), dst_name
	subprocess.run(["cp", "--reflink=auto", "--no-preserve=mode,ownership", "--",
		os.path.join(src, src_name).encode("utf8", "surrogateescape"),
		os.path.join(dst, f"{dst_name}~").encode("utf8", "surrogateescape")], check=True)
	os.rename(os.path.join(dst, f"{dst_name}~"), os.path.join(dst, f"{dst_name}"))
	if fat:
		round_mtime(os.path.join(dst, dst_name))


def copy_flac(args):
	(src, src_name, dst, dst_name, fat, format, quality) = args
	logging.info(f"Convert {src_name}.flac to {dst_name}.{format}")
	assert safe_filename(src_name), src_name
	assert safe_filename(dst_name), dst_name
	if format == "ogg":
		subprocess.run(["oggenc", "--quality", str(quality), "--discard-comments", "--quiet",
			b"--output=" + os.path.join(dst, f"{dst_name}.ogg~").encode("utf8", "surrogateescape"),
			"--", os.path.join(src, f"{src_name}.flac").encode("utf8", "surrogateescape")], check=True)
		os.rename(os.path.join(dst, f"{dst_name}.ogg~"), os.path.join(dst, f"{dst_name}.ogg"))
	elif format == "mp3":
		subprocess.run(["lame", "--quiet", "--replaygain-accurate", "-m", "s", "-V", str(quality), "-q", "0",
			os.path.join(src, f"{src_name}.flac").encode("utf8", "surrogateescape"),
			os.path.join(dst, f"{dst_name}.mp3~").encode("utf8", "surrogateescape")], check=True)
		os.rename(os.path.join(dst, f"{dst_name}.mp3~"), os.path.join(dst, f"{dst_name}.mp3"))
	else:
		raise Exception(f"Unknown format: {format}")
	sync_flac(args[:-1])


def sync_flac(args):
	(src, src_name, dst, dst_name, fat, format) = args
	assert safe_filename(src_name), src_name
	assert safe_filename(dst_name), dst_name
	src_m = mutagen.flac.FLAC(os.path.join(src, f"{src_name}.flac"))
	if format == "ogg":
		dst_fn = os.path.join(dst, f"{dst_name}.ogg")
		dst_m = mutagen.oggvorbis.OggVorbis(dst_fn)
	elif format == "mp3":
		dst_fn = os.path.join(dst, f"{dst_name}.mp3")
		dst_m = mutagen.mp3.EasyMP3(dst_fn)
		if dst_m.tags is None:
			dst_m.add_tags()
		for tag in src_m.tags.keys():
			try:
				dst_m[tag]
			except mutagen.easyid3.EasyID3KeyError:
				del src_m.tags[tag]
			except KeyError:
				pass
	else:
		raise Exception(f"Unknown format: {format}")

	src_tags = dict(src_m.tags)
	dst_tags = dict(dst_m.tags)

	if format == "mp3":
		# These can't be represented precisely in the MP3 format
		for tag in ("replaygain_track_gain", "replaygain_track_peak", "replaygain_album_gain", "replaygain_album_peak"):
			if tag in src_tags:
				del src_tags[tag]
			if tag in dst_tags:
				del dst_tags[tag]

	if src_tags != dst_tags:
		logging.debug(f"Tag {dst_name}.{format}")
		dst_m.tags.clear()
		for k, v in src_m.tags.items():
			dst_m.tags[k] = v
		subprocess.run(["cp", "--reflink=auto", "--no-preserve=mode,ownership,timestamps", "--",
			dst_fn.encode("utf8", "surrogateescape"),
			f"{dst_fn}~".encode("utf8", "surrogateescape")], check=True)
		dst_m.save(f"{dst_fn}~")
		os.rename(f"{dst_fn}~", dst_fn)

	if fat:
		round_mtime(dst_fn)


def fat_safe(text):
	assert text != ""
	while text.endswith("."):
		text = text[:-1]
	assert text != ""
	return text

def safe_chars_only(text, file=True):
	assert text != ""
	text = text.replace("P!nk", "Pink")
	text = "/".join([fat_safe(unidecode.unidecode(x).replace("/", "_")) for x in text.split("/")])
	if file:
		text = re_unsafe_f.sub("_", text)
	else:
		text = re_unsafe_d.sub("_", text)
	assert text != ""
	return text

def android_safe_chars_only(text):
	assert text != ""
	text = text.replace(":", "∶").replace("?", "⸮")
	text = re_unsafe_android.sub("_", text)
	return text


def get_title(filename):
	src = mutagen.flac.FLAC(filename)
	disc = src.tags.get("DISCNUMBER", [""])[0].split("/")[0]
	track = src.tags.get("TRACKNUMBER", [""])[0].split("/")[0]
	if disc != "":
		disc = f"{int(disc):02d}."
	if track != "":
		track = f"{int(track):02d}"
	prefix = f"{disc}{track}"
	if prefix:
		prefix += " "
	title = safe_chars_only(src.tags["TITLE"][0].replace("/", "-"))
	return f"{prefix}{title}"


def sync_paths(src, dst, format="ogg", quality=6, user=None, rewrite=False, fat=False, no_extra=False, android=False):
	access = {}
	if user is not None:
		pwnam = pwd.getpwnam(user)
		uid = pwnam.pw_uid
		groups = set([pwnam.pw_gid] + [g.gr_gid for g in grp.getgrall() if pwnam.pw_name in g.gr_mem])

	def _has_access(path, target=False):
		if user is None:
			return True

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

	for root, dirs, files in os.walk(src, followlinks=True):
		for name in files:
			name = os.path.join(root, name)
			if filter_file(name, no_extra) and _has_access(name):
				walk_dir = os.path.dirname(name)
				while walk_dir != src:
					dir_name = os.path.relpath(walk_dir, src)
					if rewrite:
						modified_dirname = safe_chars_only(dir_name, False)
					elif android:
						modified_dirname = android_safe_chars_only(dir_name)
					else:
						modified_dirname = dir_name
					src_dirs.add(modified_dirname)
					walk_dir = os.path.dirname(walk_dir)

				src_files.add(os.path.relpath(name, src))

	for root, dirs, files in os.walk(dst):
		for name in dirs:
			name = os.path.join(root, name)
			dst_dirs.add(os.path.relpath(name, dst))
		for name in files:
			name = os.path.join(root, name)
			dst_files.add(os.path.relpath(name, dst))

	src_flac_files = set()
	src_flac_map_files = {}
	src_as_format_files = set()
	src_not_flac_files = set()
	src_not_flac_map_files = {}
	for name in src_files:
		parts = name.split(".")
		if parts[-1] == "flac":
			name = ".".join(parts[0:-1])
			if rewrite:
				modified_name = os.path.join(safe_chars_only(os.path.dirname(name), False), get_title(os.path.join(src, f"{name}.flac")))
			elif android:
				modified_name = android_safe_chars_only(name)
			else:
				modified_name = name
			src_flac_files.add(modified_name)
			src_flac_map_files[modified_name] = name
			src_as_format_files.add(f"{modified_name}.{format}")
		else:
			if rewrite:
				modified_name = safe_chars_only(name, False)
			elif android:
				modified_name = android_safe_chars_only(name)
			else:
				modified_name = name
			src_not_flac_files.add(modified_name)
			src_not_flac_map_files[modified_name] = name
			src_as_format_files.add(modified_name)

	dst_format_files = set()
	for name in dst_files:
		parts = name.split(".")
		if parts[-1] == format:
			name = ".".join(parts[0:-1])
			dst_format_files.add(name)

	for name in src_flac_files & dst_format_files:
		src_mtime = os.lstat(os.path.join(src, f"{src_flac_map_files[name]}.flac")).st_mtime
		dst_mtime = os.lstat(os.path.join(dst, f"{name}.{format}")).st_mtime
		if src_mtime >= dst_mtime:
			logging.debug(f"Refresh {src_flac_map_files[name]}.flac ({src_mtime} >= {dst_mtime})")
			os.unlink(os.path.join(dst, f"{name}.{format}"))
			dst_format_files.remove(name)
			dst_files.remove(f"{name}.{format}")

	for name in src_not_flac_files & dst_files:
		src_mtime = os.lstat(os.path.join(src, src_not_flac_map_files[name])).st_mtime
		dst_mtime = os.lstat(os.path.join(dst, name)).st_mtime
		if src_mtime >= dst_mtime:
			logging.debug(f"Refresh {name} ({src_mtime} >= {dst_mtime})")
			os.unlink(os.path.join(dst, name))
			dst_files.remove(name)

	for name in dst_files - src_as_format_files:
		logging.info(f"Delete file {name}")
		assert safe_filename(name)
		os.unlink(os.path.join(dst, name))

	for name in dst_dirs - src_dirs:
		logging.info(f"Delete dir {name}")
		assert safe_filename(name)
		shutil.rmtree(os.path.join(dst, name), ignore_errors=True)

	for name in src_dirs - dst_dirs:
		logging.debug(f"Create dir {name}")
		assert safe_filename(name)
		os.makedirs(os.path.join(dst, name), exist_ok=True)

	if fat:
		for name in src_dirs & dst_dirs:
			round_mtime(os.path.join(dst, name))

	with mp.Pool(os.cpu_count()) as p:
		p.map(copy_file, [(src, src_not_flac_map_files[name], dst, name, fat) for name in (src_not_flac_files - dst_files)])
		p.map(copy_flac, [(src, src_flac_map_files[name], dst, name, fat, format, quality) for name in (src_flac_files - dst_format_files)])
		p.map(sync_flac, [(src, src_flac_map_files[name], dst, name, fat, format) for name in (src_flac_files & dst_format_files)])


if __name__ == "__main__":
	mp.current_process().name = "Main"

	parser = argparse.ArgumentParser(description="Convert music from FLAC to a lower bitrate format")
	parser.add_argument("--src", metavar="PATH", type=str, required=True, help="Source path")
	parser.add_argument("--dst", metavar="PATH", type=str, required=True, help="Destination path")
	parser.add_argument("--format", metavar="FORMAT", type=str, default="ogg", help="Encoding format")
	parser.add_argument("--quality", metavar="QUALITY", type=int, default=6, help="Encoding quality")
	parser.add_argument("--user", metavar="USER", type=str, help="Ignore source files that are not accessible by USER")
	parser.add_argument("--rewrite", action="store_true", help="Rewrite filenames to be safe and use titles")
	parser.add_argument("--fat", action="store_true", help="Round output file timestamps up")
	parser.add_argument("--no-extra", action="store_true", help="No extra files")
	parser.add_argument("--android", action="store_true", help="Android compatibility")

	args = parser.parse_args()
	logging.debug("start")
	sync_paths(args.src, args.dst, args.format, args.quality, args.user, args.rewrite, args.fat, args.no_extra, args.android)
	logging.debug("stop")
