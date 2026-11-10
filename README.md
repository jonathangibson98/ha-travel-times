# Jon's Home Assistant Apps

A collection of custom Home Assistant add-ons (apps). Add this repository once and every app below shows up in the Add-on Store.

---

## 🚀 Included Apps

| | App | What it does | Needs |
|---|---|---|---|
| 🤖 | [**Creator & AI Builder**](ai_builder) | Describe what you want in plain English and your `ai_task` entity builds automations, scenes, scripts, template sensors, helpers, AppDaemon apps, pyscripts, blueprints and even add-ons. Also makes AI voice announcements, turns photos into SVG icons, and backs up its own data. | An `ai_task` entity (a TTS engine and speaker for announcements) |
| 📄 | [**File Reader**](file_reader) | Web UI and MQTT file manager. Every text file in your folder becomes a Home Assistant sensor (last line as the state), plus an optional REST API for automations and logging. | MQTT |
| ☁️ | [**iCloud Lite**](ha-icloud-lite) | Track Apple devices from multiple iCloud accounts. Each device becomes a `device_tracker` with a battery sensor. | MQTT |
| 📍 | [**Location Hub**](location_hub) | A location history map (trails, playback, stops, places, heatmap) plus everyone's location sensor (**you choose its name**), switches, device trackers, a dashboard card generator and AI arrival alerts. One add-on, one sidebar entry. | MQTT; an `ai_task` entity and a `notify.*` service for alerts |
| 👤 | [**Personalized Info**](personalized_info) | Edit everyone's info and music preferences from a phone-friendly UI. Saved to one readable YAML file with automatic backups. | none (MQTT optional) |
| 🖼️ | [**Photo Hub**](photo_shuffle) | Folder shuffles and random Unsplash photos, all published as image entities. Manage everything from one web UI. | MQTT (Unsplash key for Unsplash collections) |
| 🎬 | [**Release Tracker**](release-tracker) | Pick the TV shows and movies you care about and get sensors for what is coming out and when. | Free TMDB API key |
| 🏈 | [**Team Tracker Lite**](team_tracker_lite) | One lightweight live sports sensor. Pick any team from a menu, with a calendar of every game for your saved teams. | MQTT |
| 🚗 | [**Travel Times**](travel_times) | Live drive-time sensors (Waze) from each person to each destination, polled faster while someone is moving. Pick people and destinations in the web UI. | MQTT |
| 🎙️ | [**Voice Chat**](voice_chat) | Hands-free voice conversation with any Home Assistant AI agent, answered out loud on the speaker you choose. | A conversation agent, a TTS engine and a media player |
| ⏱️ | [**Work Hours Tracker**](work_hours) | Logs how long each person spends in the work zones you pick for them, saved as one file per person, with MQTT sensors. | Home Assistant zones (MQTT optional) |

Each app has its own **Documentation** tab in Home Assistant, and a `DOCS.md` / `README.md` in its folder with setup details.

---

## 🛠️ Installation in Home Assistant

1. Open **Home Assistant**.
2. Go to **Settings** -> **Add-ons** -> **Add-on Store**.
3. Click the three dots in the upper right corner and select **Repositories**.
4. Add the URL of this repository:

   ```text
   https://github.com/jonathangibson98/my-home-assistant-apps
   ```

5. Close the dialog, refresh the page, and the apps appear in the store.
6. Pick one, click **Install**, then **Start**. Turn on **Show in sidebar** for apps that have a web UI.

### Apps that use MQTT

Install the **Mosquitto broker** add-on and set up the **MQTT** integration first. The apps detect the broker automatically, so you normally don't need to enter any MQTT details.

---

## 📦 Prebuilt images (GitHub packages)

An add-on whose `config.yaml` has an `image:` line (Location Hub does) is downloaded by Home Assistant as a finished image from GitHub's container registry instead of being built on your Pi. Add-ons without that line are still built locally on install.

Publishing an image:

1. Bump `version` in the add-on's `config.yaml` and commit.
2. Push a tag that matches it, for example `git tag v2.4.0 && git push origin v2.4.0`. The **Publish prebuilt images to GHCR** action (`.github/workflows/create.yml`) builds every add-on for `aarch64` and `amd64` and pushes them to `ghcr.io/jonathangibson98/<arch>-addon-<slug>`, tagged with the version and `latest`. You can also start it by hand from the **Actions** tab.
3. On GitHub open your profile, **Packages**, the new package, **Package settings**, and set it to **Public**. Home Assistant cannot pull a private package.

The tag must match the add-on version, or Home Assistant will look for an image tag that was never built.

---

## 📂 Repository layout

```text
my-home-assistant-apps/
├── .github/workflows/create.yml
├── ai_builder/
├── file_reader/
├── ha-icloud-lite/
├── location_hub/
├── personalized_info/
├── photo_shuffle/        (Photo Hub)
├── release-tracker/
├── team_tracker_lite/
├── voice_chat/
├── work_hours/
└── repository.yaml
```
