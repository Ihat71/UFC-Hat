# 🥊 UFC-HAT: Data Analysis and Evaluation of UFC Fighters

## 📌 Overview
This project analyzes and evaluates UFC fighters using historical fight data from ESPN and official UFC records.  
The goal is to rank fighters using an ELO-based system that considers **who a fighter defeated and when they defeated them**.

For example, defeating a rookie superstar should not be rewarded the same as defeating that same fighter during their prime.

This project is exploratory and designed to generate interesting insights from combat sports data.

---

## 🎯 Objectives
- Evaluate UFC fighters using authentic domain knowledge
- Identify the true greats of the sport
- Rank fighters based on skill and career greatness
- Compare fighters' statistics, skills, and careers
- Potentially predict match outcomes between fighters

---

## 📊 Dataset

**Sources**
- Official UFC Stats Website
- ESPN Fight Data
- MMA Compass

**Features Included**
- ELO rating for UFC fighters
- Detailed fighter statistics from ESPN records
- Evaluation of striking, grappling, clinch, and career performance
- Data visualizations and analytical plots
- Fighter matchup comparisons
- *(In progress)* Fight outcome predictor

---

## 🛠️ Tech Stack
- Python
- Flask / Werkzeug
- Pandas
- NumPy
- Matplotlib / Plotly
- BeautifulSoup / Selenium
- Scikit-learn

---

## 🔎 Data Cleaning
- Handled missing values
- Converted time formats
- Aggregated fight statistics
- Removed duplicates
- Feature engineering
- Normalized features

---

## 📈 Exploratory Data Analysis (EDA)
- Striking vs grappling vs clinch dominance
- Opponent quality and strength of schedule
- Well-rounded fighters vs specialized fighters
- High-volume winners vs high-quality winners

---

## 💡 Key Insights
- Grappling shows strong statistical dominance, while KO power acts as a major X-factor
- Most fights end in decisions rather than finishes
- Top-ranked fighters (current model results):
  - Jon Jones
  - Georges St-Pierre (GSP)
  - Islam Makhachev
- Skill distributions vary significantly across weight classes

---

## 🚀 Future Improvements
- Analyze long-term fighting trends
- Build a deep learning fight prediction model
- Improve feature engineering pipeline
- Deploy as a web application
- Expand analytical insights

---

## 📂 Project Structure

ufc-hat/
│
├── data/ # Raw and processed datasets <br />
├── my_app/ # Application source code <br />
├── tests/ # Unit tests <br />
├── notebooks/ # Jupyter notebooks for analysis <br />
├── README.md <br />
└── requirements.txt <br />

---