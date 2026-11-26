# Bringing upstream's playback upgrades into BeeMTube2 (Beta 6, not started)

Measured on 2026-11-25 against `JustAnotherPU/ridetube` at `eb0973b` (app 0.6.4, service 0.5.4). Our fork left
upstream at `6336494` (service 0.3.9). Nothing here is merged yet: `main` is Beta 5.

## What a trial merge showed

`git merge up/main`: 13 files in conflict, 68 places. Kept to the service and the decoder's source (the app and
the kit left as ours): 5 files, 16 places in `native/jap-ridetube-d/ytd.js`, 4 in its test.

The 16 places in `ytd.js` are not independent. Upstream rewrote two things our iDrive 8 work also rewrote:

1. **The `Audio` class** (2 places, 50 and 296 of our lines against 112 and 93 of theirs). Upstream now describes a
   missing SOME/IP service from the unit's catalog (`descriptions`, `init`, `dropLeftovers`, `why`, `unsupported`).
   Ours finds the descriptions of iDrive 8's MainApp-21 and hands the play to `sound.js`. Ours must stay whole;
   every call of upstream's new methods elsewhere in the file has to be mapped onto ours.
2. **How a play starts, pauses, resumes and restarts** (`startAudio`, `replayAt`, `restart`, `waiting`,
   `headFile`). Our hooks sit in exactly those functions: `audio.holds()` (resume in place), the head file's switch
   to the whole file, "No sound?" (`noSound`), `diag`, `soundreset`, the state's `snd`.

So the playback upgrades cannot be taken without re-fitting the iDrive 8 sound hooks into upstream's new flow.
That is the code confirmed on the M2 and the M3 (pause, resume, the head's switch).

## What is safe about it

- Streaming keeps the **audio a whole file on flash** (only the video window is in RAM), so the file the car's
  player opens stays what it is today.
- `sound.js` and `sdkread.js` are ours alone: no conflict.
- yt-dlp is the same version in both (2026.10.26): nothing to gain there today.

## What changes under a car that works

- **The decoder binary `ytdec`** (streaming and VP9 need upstream's new one, sha256 `0c5f30e4...` in their 0.5.4
  package). The one on the M2 and M3 is 0.3.9's.
- `ytfetch.py` (merges without conflict, +1100 lines).

## Order of work

1. Resolve `ytd.js` with ours kept for `Audio`, upstream's flow for the rest, our hooks re-fitted; `test_sound.js`
   (its two tests through the daemon cover pause, resume, the head's switch and "No sound?") and upstream's
   `test_ytd.js` both have to pass.
2. Package: upstream's `ytdec` and the new service files in `component.json`, pinned in `tools/release_id8.sh`.
3. A service version upstream cannot collide with (theirs is 0.5.4 under the same id).
4. Keep Beta 5 as the release `beta5`; publish as Beta 6; confirm on a car before anything else is built on it.
5. Then the app: resume at the furthest point, playlists, downloads (21 places in `main.js`).
