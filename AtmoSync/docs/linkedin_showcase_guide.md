# AtmoSync — GitHub Pages Deployment & LinkedIn Showcase Kit

This guide provides everything you need to **deploy your live interactive dashboard to GitHub Pages** and **publish high-impact portfolio posts on LinkedIn** that capture recruiter and industry attention.

---

## 1. Accessing Your Dashboard Right Now (Local)

Your dashboard is **actively running live** on your local machine:
- **Local URL**: [http://localhost:8080](http://localhost:8080)
- **Local File Path**: [`web/index.html`](file:///web/index.html)

To restart the server at any time:
```bash
python web_server.py 8080
```
Then navigate to `http://localhost:8080` in your browser.

---

## 2. Deploying to GitHub Pages (Free Instant Cloud Hosting)

Your dashboard is equipped with an **In-Browser Edge Simulation Engine**. When hosted on GitHub Pages (static hosting with zero backend servers), it automatically executes real-time IoT fleet physics, Arrhenius kinetics, Leaflet GIS mapping, Chart.js graphs, and financial arbitrage calculations directly in the browser!

### Method A: Automated GitHub Actions (Recommended)
A ready-to-run GitHub Actions workflow has already been configured in [`.github/workflows/deploy-pages.yml`](file:///.github/workflows/deploy-pages.yml).

1. Push your repository to GitHub:
   ```bash
   git add .
   git commit -m "feat: complete interactive dashboard with github pages & linkedin kit"
   git push origin main
   ```
2. On GitHub, navigate to **Settings** &rarr; **Pages**.
3. Under **Build and deployment** &rarr; **Source**, select **GitHub Actions**.
4. The workflow will automatically trigger, build, and publish your live site.
5. Your live URL will be:
   ```
   https://<your-github-username>.github.io/<your-repository-name>/
   ```

### Method B: Deploy from Branch (30-second setup)
1. In your GitHub repository, go to **Settings** &rarr; **Pages**.
2. Under **Build and deployment** &rarr; **Source**, select **Deploy from a branch**.
3. Under **Branch**, select `main` (or `master`) and folder `/(root)`.
4. Click **Save**. Within 1–2 minutes, your live site will be accessible!

---

## 3. LinkedIn Post Templates (Ready-to-Post)

### Post Option 1: Data Engineering & Distributed Systems Focus
> 🚛 **How do you save ₹280,000+ of perishable cargo before it spoils inside a shipping container?**
>
> In cold-chain logistics across India, a single compressor fault causes silent thermal runaway. Standard tracking only alerts operators after dock delivery—when 100% of the produce is written off.
>
> I built **AtmoSync**, an end-to-end Micro-Climate Arbitrage Analytics engine that connects:
> 📡 **IoT Edge Simulators**: Real-time telematic streaming of temp, humidity, vibration G-force & GPS every 2s into **Apache Kafka (KRaft)**
> ❄️ **Snowflake Warehouse**: Centralized RAW telemetry ingestion joined with dynamic APMC mandi spot prices
> ⚙️ **dbt Core**: Modular ELT transformation calculating non-linear **Arrhenius Thermal Spoilage Kinetics** and dynamic Time-to-Spoilage (TTS)
> 📊 **Arbitrage Decision Tower**: Evaluates alternate terminal markets in real-time, executing profitable diversions before degradation thresholds breach.
>
> 🔗 Live Interactive Dashboard: https://<your-github-username>.github.io/<your-repo>/
> 💻 GitHub Repository: https://github.com/<your-github-username>/<your-repo>
>
> #DataEngineering #ApacheKafka #Snowflake #dbt #IoT #SupplyChain #Python #DataAnalytics

---

## 4. Key Interactive Dashboard Features to Showcase

1. **Live Operational Tower**: Real-time Leaflet GIS mapping across freight corridors (Nashik, Mumbai, Pune, Bengaluru, Delhi, Chennai).
2. **Kinetics & Spoilage Leaderboard**: Live dynamic Arrhenius thermal decay curves and risk scores (0–100).
3. **Multi-Market Arbitrage Matrix**: Real-time dock-loss salvage calculation vs. alternate market diversion revenue.
4. **Emergency Reroute Execution**: 1-click incident dispatcher resolving high-risk container alerts.
5. **Interactive Mathematical Sandbox**: Telemetry sliders to experiment with Arrhenius degradation formulas.
