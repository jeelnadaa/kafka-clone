# Hyperframes Composition Brief: Kafka Clone

## Objective
Create a short launch-style brag video for Kafka Clone.

## Output
- Composition directory: `brag-output/composition/`
- Rendered video: `brag-output/brag.mp4`
- Format: landscape — 1920x1080
- Duration: 19 seconds

## Source Material
- Project root: `d:/kafka-clone/cleaned`
- Primary files read: `README.md`, `web/index.html`, `web/style.css`, `web/app.js`
- Product name: Kafka Clone
- Tagline / strongest claim: "Append-Only Distributed Message Broker & Storage Engine built from first principles in Java with zero external dependencies."
- Key UI or visual moment to recreate:
  - Neo-brutalist header card and cluster status metrics (TCP 9092, WEB 8080, TOTAL MSGS, UPTIME)
  - Live Broker Studio with Produce form and instantaneous ACK response badge
  - Disk Log & Index Inspector with binary `.log` and `.index` O(log N) offset mapping
  - Consumer Group Playground with committed offset tracking to `__consumer_offsets.dat`
- Copy that must appear verbatim:
  - "TIRED OF 400 DEPENDENCIES?"
  - "Append-Only Distributed Message Broker & Storage Engine"
  - "NIO FileChannel // Binary Wire Protocol // O(log N) Index"
  - "ACK RECEIVED // Offset: #42 // CRC32: 0x8F3A21BC"
  - "00000000000000000000.log"
  - "00000000000000000000.index"
  - "Built from first principles. Zero dependencies."

## Creative Direction
- Tone preset: default
- Creative direction: Neo-brutalist systems engineering flex
- Interpretation: Fast, energetic, tactile, high-contrast. Thick black borders, un-blurred solid drop shadows, vivid neo-brutalist colors (#FFE600 yellow, #00F0FF cyan, #FF70A6 pink, #70FF00 lime).
- Angle: Who needs Apache's 400 dependencies? Rebuilt Kafka's core storage engine and protocol from scratch with pure Java NIO.
- Hook: "TIRED OF 400 DEPENDENCIES? We rebuilt Apache Kafka from scratch."
- Outro / punchline: "Kafka Clone. Append-Only. Distributed. Pure Java."
- Avoid:
  - Generic SaaS language ("streamline your workflows")
  - Abstract filler visuals (generic floating 3D spheres or disconnected particle effects)
  - Unrelated visual redesigns (preserve the project's authentic Neo-Brutalist UI)

## Visual Identity
- Background: `#FFFDF0` with 24px radial dot grid (`radial-gradient(#000000 1.2px, transparent 1.2px)`)
- Text: `#000000` (deep black, crisp font weight)
- Accent: Neo-Brutalist palette (`#FFE600`, `#00F0FF`, `#FF70A6`, `#70FF00`, `#FF9F1C`)
- Borders: `3px` to `4px` solid `#000000`
- Shadows: `5px 5px 0px #000000`
- Display font: Public Sans / Space Grotesk
- Body font: Space Grotesk
- Mono font: JetBrains Mono
- Visual references from the project: Marquee ticker banner, brutalist badges, metric cards, live form controls, disk segment cards.

## Storyboard
Scene summary (total duration: ~19s):
1. **The Hook (0.0s – 3.2s)** — Neo-brutalist hero card, scrolling ticker marquee, punchy problem statement "TIRED OF 400 DEPENDENCIES?", badges `[JAVA NIO]`, `[ZERO LIBS]`, `[RAW BYTES]`.
2. **Live Broker Studio & Produce Flow (3.2s – 8.2s)** — Recreates the live studio. Message payload `orders:0` is sent; simulated click triggers instant binary TCP serialization, FileChannel write, and lime green ACK badge `#42`.
3. **Disk Log & Index Inspector (8.2s – 13.5s)** — Physical disk segment inspection: `.log` append-only storage, `.index` binary offset lookup, and consumer offset commit to `__consumer_offsets.dat`.
4. **Dashboard Explorer & Outro (13.5s – 19.0s)** — Cluster Status Card with online pulsing dot, cluster port badges, final punchline, and clean outro.

## Audio
- Audio role: Upbeat rhythmic tech bed with conversational voiceover and tactile UI sound effects
- Audio arc: Opens snappy and energetic, carries steady momentum through the live app demos, settles on a satisfying outro hit.
- Music: `assets/music/happy-beats-business-moves-vol-1-by-ende-dot-app.mp3`
- Music treatment: Starts at 0.35 volume, ducks to 0.15 during voiceover, fades out in the last 0.8s.
- Music cue guidance: 120.19 BPM track. Major beat points at 3.02s, 8.02s, 13.51s, 17.02s, 18.52s.
- Audio-reactive treatment: subtle; card shadow and border glow pulse gently with RMS energy.
- Audio-coupled moments:
  - 1.2s: Badge reveals (pop/drop SFX)
  - 4.2s: Simulated "SEND TO BROKER" click (mouse click SFX)
  - 5.0s: ACK received card arrival (success impact bell SFX)
  - 8.5s: Disk segment cards slide in (card slide SFX)
  - 17.5s: Final brand card landing (impact bell SFX)
- Voiceover: Enabled via Kokoro TTS (`voiceover.wav` generated via Kokoro), placed on audio track 3.
- SFX files: Copied into `composition/assets/sfx/` from the brag audio library.
