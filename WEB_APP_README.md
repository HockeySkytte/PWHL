# 🏒 PWHL Analytics Web App

A professional web application for analyzing Professional Women's Hockey League (PWHL) data, designed with the same sleek aesthetic as your Shot Plotter application.

## ✨ Features

### 🎯 **Current Functionality**
- **Schedule Tab**: Complete game schedule with advanced filtering
- **Real-time API Integration**: Direct data from PWHL API
- **Professional Dark Theme**: Matches your Shot Plotter design
- **Advanced Filtering**:
  - Season selection (2023/24 & 2024/25, Regular Season & Playoffs)
  - Month filtering
  - Team filtering  
  - Game status filtering
- **Live Statistics**: Total games, completed games, pending games, average goals
- **Data Export**: CSV export functionality
- **Responsive Design**: Works on desktop and mobile

### 📊 **Data Displayed**
- Date
- Season State (Regular Season/Playoffs)
- Away Team vs Home Team
- Game Status (Final, Final OT, Final SO, etc.)
- Final Scores

## 🚀 **How to Run**

### **Option 1: Double-click Launch**
- Windows: Double-click `start_app.bat`
- PowerShell: Run `start_app.ps1`

### **Option 2: Command Line**
```bash
cd "c:\Users\larss\Apps\PWHL"
C:/Users/larss/Apps/PWHL/.venv/Scripts/python.exe flask_app.py
```

### **Option 3: Direct Python**
```bash
python flask_app.py
```

The app will be available at: **http://localhost:8501**

## ☕ **Buy Me a Coffee (Stripe)**

This app includes a **/coffee** page that starts a **Stripe Checkout** session for a one-time tip.

### **1) Create / get your Stripe keys**
- In Stripe Dashboard → **Developers** → **API keys**
- Copy your **Secret key** (starts with `sk_...`)

### **2) Configure environment variables**
Required:
- `STRIPE_SECRET_KEY` (your Stripe secret key)

Optional:
- `STRIPE_CURRENCY` (default: `usd`)
- `STRIPE_PRODUCT_NAME` (default: `PWHL Analytics - Coffee`)

PowerShell example (dev):
```powershell
$env:STRIPE_SECRET_KEY = "sk_test_..."
$env:STRIPE_CURRENCY = "usd"
$env:STRIPE_PRODUCT_NAME = "PWHL Analytics - Coffee"
```

### **3) Run the app and open the page**
- Go to: `http://localhost:8501/coffee`

Notes:
- The page currently offers fixed tip amounts ($3 / $5 / $10 / $20 / $50).
- For production deployments (Render/Heroku/etc.), set the env vars in your hosting provider.

## 🎨 **Design Philosophy**

The app is designed to match your Shot Plotter application with:
- **Dark theme** (#1e2329 background, #2d3748 panels)
- **Professional color scheme** (Blues, grays, whites)
- **Clean typography** (System fonts)
- **Intuitive layout** (Sidebar filters, main content area)
- **Responsive design** (Mobile-friendly)

## 🔧 **Technical Stack**

- **Backend**: Flask (Python web framework)
- **Frontend**: HTML5, CSS3, Vanilla JavaScript
- **Data Source**: PWHL Official API
- **Styling**: Custom CSS matching Shot Plotter theme
- **Charts**: Ready for Plotly.js integration

## 📁 **File Structure**

```
PWHL/
├── flask_app.py          # Main Flask application
├── templates/
│   └── index.html        # Frontend HTML template
├── start_app.bat         # Windows launcher
├── start_app.ps1         # PowerShell launcher
├── scraper.py            # Original data scraper
├── visualizer.py         # Static visualization generator
├── pwhl_analysis.ipynb   # Jupyter notebook analysis
└── requirements.txt      # Python dependencies
```

## 🎯 **Features Ready for Expansion**

The app is architected to easily add:
- **Team Performance Tab**: Win/loss records, statistics
- **Player Statistics Tab**: Individual player metrics
- **Game Analysis Tab**: Shot charts, advanced analytics
- **Playoff Brackets**: Tournament visualization
- **Live Updates**: Real-time score updates
- **Historical Trends**: Multi-season comparisons

## 🔄 **API Integration**

- **Automatic data refresh** from PWHL API
- **Season support**: All available seasons (2023/24 - 2024/25)
- **Real-time filtering** without page reloads
- **Error handling** for API unavailability
- **Data caching** for improved performance

## 📱 **User Experience**

- **Fast loading**: Direct API integration
- **Intuitive filters**: Easy season/team/month selection  
- **Export functionality**: Download filtered data as CSV
- **Status indicators**: Visual game status badges
- **Responsive tables**: Horizontal scroll on mobile
- **Professional feel**: Matches existing hockey analytics tools

## 🛠️ **Development Notes**

- **Modular design**: Easy to add new tabs/features
- **Clean separation**: Backend API logic separate from frontend
- **Extensible filtering**: New filter types can be easily added
- **Theme consistency**: CSS variables for easy color scheme changes
- **Cross-platform**: Works on Windows, Mac, Linux

## 🎭 **Customization**

The app can be easily customized by modifying:
- `templates/index.html` - Frontend layout and styling
- `flask_app.py` - Backend logic and API endpoints
- CSS variables in the `<style>` section for color theme changes

---

**Your PWHL Analytics app is now ready! 🏒✨**

The interface matches your Shot Plotter design and provides a professional foundation for hockey analytics. The schedule tab is fully functional with live PWHL data, and the architecture is ready for additional features like player stats, shot charts, and advanced analytics.