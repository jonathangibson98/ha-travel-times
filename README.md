# 🚗 Home Assistant Travel Times Add-on (`ha-travel-times`)

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/username/ha-travel-times/create.yml?branch=main&style=for-the-badge)](https://github.com/username/ha-travel-times/actions)
[![Home Assistant Add-on](https://img.shields.io/badge/Home%20Assistant-Add--on-blue?style=for-the-badge&logo=home-assistant)](https://www.home-assistant.io/)
[![License](https://img.shields.io/badge/License-MIT-green.style=for-the-badge)](LICENSE)

An intuitive, powerful **Home Assistant Add-on** that calculates real-time travel times, traffic delays, and route details between dynamic or fixed locations. Monitor daily commute times, set up smart notifications before heading out, or trigger home automations based on estimated arrival times.

---

## ✨ Features

- **Real-Time Traffic Monitoring**: Fetch live traffic conditions and estimated travel durations.
- **Dynamic Origin & Destination**: Track stationary locations (e.g., Home, Work) or dynamic entities (e.g., `person` or `device_tracker` entities).
- **Home Assistant Native Integration**: Exposes sensors directly into Home Assistant via MQTT or API integration.
- **Automated Workflows**: Built-in GitHub Actions (`create.yml`) for automated container building and release management.
- **Multilingual Support**: Fully localizable interface with base English support (`en.yaml`).
- **Lightweight Container**: Fast startup and low memory footprint powered by Python and Alpine Linux.

---

## 📂 Repository Structure

Below is an overview of the repository files and directory layout:

```text
ha-travel-times-main/
├── .github/
│   └── workflows/
│       └── create.yml         # GitHub Actions deployment and build pipeline
├── travel_times/
│   ├── app/
│   │   └── main.py            # Core Python application logic and API handler
│   ├── translations/
│   │   └── en.yaml            # UI and configuration translation strings
│   ├── CHANGELOG.md           # Version release notes and update logs
│   ├── DOCS.md                # In-depth add-on documentation and guides
│   ├── Dockerfile             # Container definition for the add-on
│   ├── config.yaml            # Add-on configuration schema and defaults
│   ├── icon.png               # Add-on store icon
│   ├── logo.png               # Add-on UI header logo
│   └── run.sh                 # Docker container entrypoint bash script
├── README.md                  # Main repository overview and setup guide
└── repository.yaml            # Home Assistant repository manifest
```

---

## 🚀 Installation

### Option 1: Add Repository via Home Assistant UI (Recommended)

1. Open your Home Assistant instance.
2. Navigate to **Settings** > **Add-ons** > **Add-on Store**.
3. Click the three dots in the top right corner and select **Repositories**.
4. Add the URL of this GitHub repository:
   ```text
   https://github.com/your-username/ha-travel-times
   ```
5. Click **Add**, close the dialog, and refresh the store page.
6. Scroll down to find **Travel Times** and click **Install**.

### Option 2: Manual Installation

1. Copy the `travel_times` folder into your Home Assistant `/addons/` directory.
2. Navigate to **Settings** > **Add-ons** > **Add-on Store**.
3. Click the menu icon (top right) and select **Check for updates**.
4. Select **Travel Times** under **Local Add-ons** and click **Install**.

---

## ⚙️ Configuration

Before starting the add-on, you must configure the basic settings in the add-on's **Configuration** tab.

### Example Configuration (`config.yaml`)

```yaml
api_key: "YOUR_ROUTING_SERVICE_API_KEY"
update_interval: 300 # In seconds (default: 5 minutes)
units: "metric"      # Options: metric, imperial

destinations:
  - name: "Work"
    origin: "zone.home"
    destination: "123 Business St, Tech City"
    travel_mode: "driving"
  - name: "Gym"
    origin: "person.john"
    destination: "456 Fitness Ave, Tech City"
    travel_mode: "bicycling"
```

### Configuration Options

| Option | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `api_key` | `string` | **Yes** | `""` | API key for your chosen routing/mapping provider. |
| `update_interval` | `integer` | No | `300` | Frequency (in seconds) to check travel times. |
| `units` | `string` | No | `metric` | Measurement system (`metric` or `imperial`). |
| `destinations` | `list` | **Yes** | `[]` | List of routes to monitor. |
| `destinations[].name` | `string` | **Yes** | `""` | Friendly name for the resulting sensor entity. |
| `destinations[].origin` | `string` | **Yes** | `""` | Starting address, lat/long, or HA entity ID. |
| `destinations[].destination` | `string` | **Yes** | `""` | Ending address, lat/long, or HA entity ID. |
| `destinations[].travel_mode`| `string` | No | `driving` | Mode of travel (`driving`, `transit`, `bicycling`, `walking`). |

---

## 🤖 Automation Examples

### Send a Push Notification if Commute Time Exceeds Threshold

```yaml
alias: "Commute Delay Alert"
description: "Notify if morning commute takes longer than 45 minutes"
trigger:
  - platform: numeric_state
    entity_id: sensor.travel_time_work
    above: 45
    for:
      minutes: 5
condition:
  - condition: time
    after: "07:00:00"
    before: "09:00:00"
action:
  - action: notify.mobile_app_your_phone
    data:
      title: "Traffic Alert 🚗"
      message: "Commute to work is currently taking {{ states('sensor.travel_time_work') }} minutes due to heavy traffic."
```

---

## 🛠️ Development & Local Testing

If you'd like to contribute or modify the application locally:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/ha-travel-times.git
   cd ha-travel-times/travel_times
   ```

2. **Build the Docker container locally:**
   ```bash
   docker build -t local/ha-travel-times .
   ```

3. **Run container manually:**
   ```bash
   docker run --rm -it -e API_KEY="your_key" local/ha-travel-times
   ```

---

## 📖 Additional Documentation

- Refer to [DOCS.md](travel_times/DOCS.md) for deeper technical specs, troubleshooting advice, and advanced config options.
- Review [CHANGELOG.md](travel_times/CHANGELOG.md) to track recent updates and breaking changes.

---

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
