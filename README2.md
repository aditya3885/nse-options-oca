
Analogy: A Reservoir
Imagine the options market is a giant water reservoir.

PCR is the total water level in the reservoir.
¦¤PCR is how much the water level rose or fell in the last few minutes.
Intraday PCR is the ratio of fresh water flowing in from the river versus water being drained out through a dam, today.
The Detailed Breakdown


| Feature | PCR (Overall) | ¦¤PCR (Change in PCR) | Intraday PCR (from COI) |
| :--- | :--- | :--- | :--- |
| **What it is** | The ratio of all existing Puts vs. Calls. | The change in the Overall PCR value. | The ratio of *today's* new Puts vs. new Calls. |
| **What it answers** | "What is the overall market structure/mood?" | "Is the overall mood getting better or worse?" | "What are aggressive traders doing *right now*?" |
| **Calculation** | `Total Put OI / Total Call OI` | `Current PCR - Previous PCR` | `Total Change in Put OI / Total Change in Call OI` |
| **Sensitivity** | **Slow**. A large, structural number. | **Medium**. Shows the trend's momentum. | **Fast**. Very sensitive to the day's activity. |
| **Analogy** | The tide level. | Is the tide coming in or going out? | The river's current flowing into the sea. |




Of course. This is a critical question, and understanding all the possible outcomes of the `Sentiment` calculation is key to using the dashboard effectively.

The sentiment is calculated in a **two-stage process**:
1.  A **"Base Sentiment"** is determined from the current market data.
2.  This base sentiment is then refined using a **"Pressure Score"** calculated from the recent flow of Open Interest, resulting in the final, enhanced sentiment.

Here is a complete breakdown of every possible value and the logic behind it.

---

### Stage 1: Calculating the "Base Sentiment"

This is done by the `get_sentiment()` method. It looks at two main inputs from the *current* data snapshot:

*   **`diff`**: The difference in Change in OI between the ATM Call and ATM Put.
*   **`pcr`**: The overall Put-Call Ratio for the entire expiry.

The logic follows these rules in order:

| Priority | If this condition is met... | The Base Sentiment is... |
| :--- | :--- | :--- |
| 1 | `diff` is very high (> 10) **OR** `pcr` is very low (< 0.7) | **Strong Bearish** |
| 2 | `diff` is very low (< -10) **OR** `pcr` is very high (> 1.3) | **Strong Bullish** |
| 3 | `diff` is moderately high (> 2) **OR** `pcr` is low (between 0.7 and 0.9) | **Mild Bearish** |
| 4 | `diff` is moderately low (< -2) **OR** `pcr` is high (between 1.1 and 1.3) | **Mild Bullish** |
| 5 | (None of the above) | **Neutral** |

This first stage gives us one of five possible base values: `Strong Bullish`, `Mild Bullish`, `Neutral`, `Mild Bearish`, or `Strong Bearish`.

---

### Stage 2: Calculating the "Pressure Score"

This is done by the `_get_enhanced_sentiment()` method. It analyzes the **last 7 history updates** to see where the money has been flowing recently.

It calculates a `pressure_score` based on these rules for each of the 7 updates:

| OI Change Event | Pressure Effect | Score |
| :--- | :--- | :--- |
| **`PE Add`** (Put Addition) | Bullish | **+1** |
| **`CE Exit`** (Call Unwinding) | Bullish | **+1** |
| **`CE Add`** (Call Addition) | Bearish | **-1** |
| **`PE Exit`** (Put Unwinding) | Bearish | **-1** |

The score is summed up over the 7 periods. A strong positive score (e.g., `+4` or more) indicates recent Bullish Pressure. A strong negative score (e.g., `-4` or less) indicates recent Bearish Pressure.

---

### Final Output: All Possible Sentiment Values and Cases

The code combines the **Base Sentiment** with the **Pressure Score** to produce the final, enhanced sentiment. Here is the master table showing every possible outcome.

| Base Sentiment (from Stage 1) | Pressure Condition (from Stage 2) | Final Sentiment Output | What It Means |
| :--- | :--- | :--- | :--- |
| **Strong Bullish** | Pressure Score is `? 4` | **Strong Bullish (Rising)** | The market is already very bullish, and recent activity is adding even more bullish pressure. |
| **Strong Bullish** | Pressure Score is `? -4` | **Strong Bullish (Fading)** | The market is bullish, but recent activity is bearish, suggesting the trend might be losing steam or reversing. |
| **Strong Bullish** | (Pressure is neutral) | **Strong Bullish** | The market is bullish, and recent activity is balanced. |
| | | | |
| **Mild Bullish** | Pressure Score is `? 4` | **Mild Bullish (Rising)** | The market is slightly bullish, and recent activity is strongly confirming this direction. |
| **Mild Bullish** | Pressure Score is `? -4` | **Mild Bullish (Fading)** | The market is slightly bullish, but recent bearish activity is a warning sign that the trend could reverse. |
| **Mild Bullish** | (Pressure is neutral) | **Mild Bullish** | The market is slightly bullish, and recent activity is balanced. |
| | | | |
| **Neutral** | Pressure Score is `? 4` | **Neutral (Bullish Pressure)** | The overall structure is balanced, but the most recent money flow is strongly bullish, hinting at a potential upward move. |
| **Neutral** | Pressure Score is `? -4` | **Neutral (Bearish Pressure)** | The overall structure is balanced, but the most recent money flow is strongly bearish, hinting at a potential downward move. |
| **Neutral** | (Pressure is neutral) | **Neutral** | The market is completely balanced in both structure and recent flow. |
| | | | |
| **Mild Bearish** | Pressure Score is `? 4` | **Mild Bearish (Weakening)** | The market is slightly bearish, but recent bullish activity suggests the bearish trend is losing power. |
| **Mild Bearish** | Pressure Score is `? -4` | **Mild Bearish (Intensifying)** | The market is slightly bearish, and recent activity is strongly confirming and accelerating this trend. |
| **Mild Bearish** | (Pressure is neutral) | **Mild Bearish** | The market is slightly bearish, and recent activity is balanced. |
| | | | |
| **Strong Bearish** | Pressure Score is `? 4` | **Strong Bearish (Weakening)** | The market is very bearish, but recent bullish activity suggests a potential short-squeeze or bottoming process. |
| **Strong Bearish** | Pressure Score is `? -4` | **Strong Bearish (Intensifying)** | The market is already very bearish, and recent activity is adding even more bearish pressure. |
| **Strong Bearish** | (Pressure is neutral) | **Strong Bearish** | The market is bearish, and recent activity is balanced. |

This comprehensive table lists all **15 possible sentiment values** your dashboard can now display and the exact conditions required to generate each one.