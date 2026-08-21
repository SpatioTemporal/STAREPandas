"""Build the temporal STARE-PODS intro video from the executed notebook.

Scenes = 1080p PNGs composed from the notebook's real outputs/figures.
Narration = macOS `say` (Samantha) reading docs/temporal_video_script.md.
Assembly = imageio-ffmpeg's bundled ffmpeg.
"""
import base64
import os
import re
import subprocess

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import nbformat
import imageio_ffmpeg

REPO = '/Users/thatdaihaiton/Workspace/STARE/STAREPandas/stare_pods_aws_parallel'
NB = os.path.join(REPO, 'starepandas', 's3_starepods_examples_video.ipynb')
SCRIPT = os.path.join(REPO, 'docs', 'temporal_video_script.md')
WORK = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'video_build')
FINAL = os.path.join(REPO, 'stare_pods_temporal_video.mp4')
FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
VOICE, RATE = 'Samantha', 150
NAVY, INK = '#1a355e', '#16181d'

os.makedirs(WORK, exist_ok=True)

# ── 1. Pull outputs + figures out of the executed notebook ───────────────────
nb = nbformat.read(NB, as_version=4)

def cell_text(i):
    text = ''.join(o.get('text', '') for o in nb.cells[i].outputs
                   if o.output_type == 'stream')
    return '\n'.join(l for l in text.splitlines() if not l.startswith('INFO:'))

figures = []
for c in nb.cells:
    if c.cell_type != 'code':
        continue
    for o in c.outputs:
        if o.output_type in ('display_data', 'execute_result') and \
                'image/png' in o.get('data', {}):
            path = os.path.join(WORK, f'fig{len(figures)}.png')
            with open(path, 'wb') as f:
                f.write(base64.b64decode(o['data']['image/png']))
            figures.append(path)
assert len(figures) == 3, figures

# ── 2. Scene renderers ───────────────────────────────────────────────────────
def canvas():
    fig = plt.figure(figsize=(19.2, 10.8), dpi=100)
    fig.patch.set_facecolor('white')
    fig.text(0.033, 0.033, 'STARE-PODS · temporal extension',
             fontsize=15, color='#8a8f98')
    return fig

def header(fig, title):
    fig.text(0.033, 0.955, title, fontsize=36, fontweight='bold',
             color=NAVY, va='top')
    fig.add_artist(plt.Line2D([0.033, 0.967], [0.895, 0.895],
                              color=NAVY, lw=2, transform=fig.transFigure))

def text_scene(path, title, body):
    lines = body.rstrip().splitlines()
    # body box: y 0.855 down to 0.07 → ~848 px; 1 pt = dpi/72 px, 1.45 linespacing
    budget_px = 848
    size = int(budget_px * 72 / (100 * 1.45 * max(len(lines), 1)))
    size = max(12, min(22, size))
    fig = canvas()
    header(fig, title)
    fig.text(0.033, 0.855, '\n'.join(lines), fontsize=size,
             family='monospace', va='top', color=INK, linespacing=1.45)
    fig.savefig(path)
    plt.close(fig)

def figure_scene(path, title, image_path):
    fig = canvas()
    header(fig, title)
    ax = fig.add_axes([0.02, 0.055, 0.96, 0.815])
    ax.imshow(plt.imread(image_path))
    ax.axis('off')
    fig.savefig(path)
    plt.close(fig)

def title_scene(path):
    fig = canvas()
    fig.text(0.5, 0.72, 'STARE-PODS', fontsize=76, fontweight='bold',
             color=NAVY, ha='center')
    fig.text(0.5, 0.60, 'The Temporal Extension', fontsize=42,
             color=INK, ha='center')
    fig.text(0.5, 0.47, 'Every chunk knows where — now it also knows when.',
             fontsize=24, color='#555a63', ha='center', style='italic')
    fig.text(0.5, 0.33, 'GMI  ·  SSMIS  ·  AMSR2  ·  ATMS      '
             'live on AWS S3 + RDS Postgres',
             fontsize=20, color='#555a63', ha='center')
    fig.savefig(path)
    plt.close(fig)

def outro_scene(path):
    fig = canvas()
    header(fig, 'Recap')
    bullets = [
        'Every chunk carries a measured temporal range  [t_start, t_end]  +  its pod code',
        'Queries filter by period, by place, or both at once — pushed into SQL',
        'The VCF roll-up summarizes any subtree of the sky, on the fly',
        'Rendezvous analytics find every cross-instrument coincidence —',
        '    matrix · per-pod table · drill-downs — from one thin catalog load',
    ]
    fig.text(0.06, 0.76, '\n\n'.join('•  ' + b for b in bullets),
             fontsize=24, va='top', color=INK, linespacing=1.5)
    fig.savefig(path)
    plt.close(fig)

# ── 3. Compose the scenes ────────────────────────────────────────────────────
p4 = cell_text(9)
scenes = []          # (scene_png, narration_key)

s = os.path.join(WORK, 'scene0.png'); title_scene(s); scenes.append((s, 'intro'))
s = os.path.join(WORK, 'scene1.png')
text_scene(s, 'Part 1 — Every chunk knows when: the temporal catalog', cell_text(3))
scenes.append((s, 'p1'))
s = os.path.join(WORK, 'scene2.png')
text_scene(s, 'Part 2 — Ask by time: the period filter', cell_text(5))
scenes.append((s, 'p2'))
s = os.path.join(WORK, 'scene3.png')
text_scene(s, 'Part 3 — Roll it up: the temporal hierarchy (VCF)', cell_text(7))
scenes.append((s, 'p3'))
s = os.path.join(WORK, 'scene4.png')
text_scene(s, 'Part 4 — Who met whom: rendezvous analytics', p4)
scenes.append((s, 'p4'))
s = os.path.join(WORK, 'scene5.png')
figure_scene(s, 'Part 5 — Where they flew: the coverage map', figures[0])
scenes.append((s, 'p5'))
s = os.path.join(WORK, 'scene6.png')
figure_scene(s, 'Part 6 — Up close: the tightest pair (GMI + SSMIS, ~2 min apart)',
             figures[1])
scenes.append((s, 'p6a'))
s = os.path.join(WORK, 'scene7.png')
figure_scene(s, 'Part 6 — Up close: all four instruments in pod q003200', figures[2])
scenes.append((s, 'p6b'))
s = os.path.join(WORK, 'scene8.png')
text_scene(s, 'Part 7 — Where AND when: both filters in one query', cell_text(15))
scenes.append((s, 'p7'))
s = os.path.join(WORK, 'scene9.png'); outro_scene(s); scenes.append((s, 'outro'))

# ── 4. Narration from the script doc ─────────────────────────────────────────
doc = open(SCRIPT).read()
transcript = doc.split('## Transcript', 1)[1]
sections = re.split(r'^### ', transcript, flags=re.M)[1:]
order = ['intro', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'outro']
narration = {}
for key, sec in zip(order, sections):
    body = sec.split('\n', 1)[1]
    body = ' '.join(l.strip() for l in body.strip().splitlines() if l.strip())
    narration[key] = body

split_at = 'And then the star of the show'
assert split_at in narration['p6']
narration['p6a'], narration['p6b'] = (
    narration['p6'].split(split_at, 1)[0].strip(),
    split_at + narration['p6'].split(split_at, 1)[1],
)

def speakable(text):
    text = re.sub(r'\*([^*]+)\*', r'\1', text)          # markdown emphasis
    for src, spoken in [
        ('STARE-PODS', 'stare pods'), ('SSMIS', 'S S M I S'),
        ('AMSR2', 'A M S R 2'), ('ATMS', 'A T M S'), ('GMI', 'G M I'),
        ('S3', 'S 3'), ('VCF', 'V C F'), ('t-start', 'T start'),
        ('t-end', 'T end'),
    ]:
        text = text.replace(src, spoken)
    return text

# ── 5. TTS + duration ────────────────────────────────────────────────────────
def duration_of(path):
    out = subprocess.run(['afinfo', path], capture_output=True, text=True).stdout
    return float(re.search(r'estimated duration: ([\d.]+)', out).group(1))

segments = []
total = 0.0
for i, (scene, key) in enumerate(scenes):
    aiff = os.path.join(WORK, f'voice{i}.aiff')
    subprocess.run(['say', '-v', VOICE, '-r', str(RATE), '-o', aiff,
                    speakable(narration[key])], check=True)
    dur = duration_of(aiff) + (1.2 if key in ('p5', 'p6a', 'p6b') else 0.7)
    seg = os.path.join(WORK, f'seg{i}.mp4')
    subprocess.run([
        FFMPEG, '-y', '-loglevel', 'error',
        '-loop', '1', '-i', scene, '-i', aiff,
        '-c:v', 'libx264', '-tune', 'stillimage', '-pix_fmt', 'yuv420p',
        '-r', '30', '-c:a', 'aac', '-ar', '44100', '-ac', '1',
        '-af', 'apad', '-t', f'{dur:.2f}', seg,
    ], check=True)
    segments.append(seg)
    total += dur
    print(f'{key:6s} {dur:6.1f}s  {os.path.basename(scene)}')

concat = os.path.join(WORK, 'concat.txt')
with open(concat, 'w') as f:
    f.writelines(f"file '{s}'\n" for s in segments)
subprocess.run([FFMPEG, '-y', '-loglevel', 'error', '-f', 'concat',
                '-safe', '0', '-i', concat, '-c', 'copy', FINAL], check=True)
print(f'\nTOTAL {total/60:.1f} min  ->  {FINAL}  '
      f'({os.path.getsize(FINAL)/1e6:.1f} MB)')
