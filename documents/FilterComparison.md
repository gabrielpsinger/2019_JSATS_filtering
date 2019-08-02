Parameters for 5s nominal PRI | FAST | NOAA ATS/Tek | NOAA Lotek | UCD1 | UCD data.table version < 2.6 | UCD dt 2.6+ | UCD dt version 3+ (in development)
---| --- | --- | --- | --- | --- | --- | ---
Coding language, method (I=iterative S=set-based) | unknown | Matlab, I | Matlab, S? | R, I | R, S | R, S | R, S
Author | PNNL | Ammann | Ammann | Gabe+Dom | Matt | Matt | Matt
Multipath cutoff (s) |  0.156 |  0.2 |  0.0 |  0.2 |  0.2 |  0.2 |  0.2
Starting window size | 79 | 75 | 79 | 79 | 79 | 79 | 79.0
First cut PRI window lower bound |  3.255 |  4.8 |  4.1 |  3.255 |  3.255 |  3.255 |  3.255
First cut PRI window upper bound |  6.5 |  5.2 |  5.9 |  6.5 |  6.5 |  6.5 |  6.5
ePRI calculation method (general) | Per-window, per tag, per receiver (WTR) | NA | Global window, per tag, per receiver (GTR) | WTR | TR (Per tag, per receiver) | WTR | WTR
ePRI selection | Round to 2 digits, most frequent, tie to shortest PRI (R2,MF,T<) | NA | Global window, MF, no known tie-break | R2,MF,T< | R2,MF, tiebreak 1) closest PRI to nominal 2) minimum (TNM). | R2,MF,TNM | R2,MF,TNM
Acceptable PRI flop max from ePRI, single interval |  0.012 | NA |  0.2 |  0.012 |  0.31 |  0.012 |  0.012
Acceptable PRI flop max, 12 intervals |  0.078 | NA |  0.2 |  0.078 |  2.015 |  0.078 |  0.078
Hits per window? |  4 |  2 |  4 |  3 |  3 |  3 |  3 non-mimic equivalent
Technologies: ATS (autonomous) | Yes | Yes | No | Yes | Yes | Yes | Yes
Technologies: Teknologic (autonomous) | No | Yes | No | Yes | Yes | Yes | Yes
Technologies: Lotek (autonomous) | No | No | Yes | Yes? | Yes | Yes | Yes
Technologies: Realtime (specify types) | No | No | No | No | Teknologic DataCom | Teknologic (DC and ShoreStation) | Teknologic (DC and SS)
Technologies: Other databases | No | No | No | No | No | No until version 2.7 (ERDDAP via CSV) | ERDDAP
Technology recognition and handling | none | file extension | file extension | file extension | file extension | file extension (versions below 2.7); auto recognition 2.7+ | automatic recognition top 20 lines if CSV/TXT; otherwise extension-based
Mimic code handling? | Yes? | No | No | No | No | No | Planned

Comments
--------
PNNL+UCD1 -- selection of minimum mode PRI biases toward e.g. 3.5 seconds for 3 hit windows and may result in both false positives and false negatives (tossed out real data)

Ammann (NOAA) -- first pass requires consecutive hits to be at least 4 seconds apart (no maximum), second pass requires 4.8<PRI<5.2. Both passes operate on nPRI, so compared to other algorithms are only "first cut"

Ammann (NOAA) -- with a 2-hit filter, PRIs are largely irrelevent: all hits that make it past first crude filter are kept, regardless of ePRI calculation. ATS/Tekno code has no futher logic to examine ePRIs

Ammann (NOAA) -- Global window takes the difference from any one hit to the very first hit of that tag, assumes a near-nominal PRI, finds the corresponding number of iterations rounded to the nearest integer, then sets PRI to the mode of time_since_first/iterations (rounded to two decimals). This will result in any tag having a long time gap between first detection and last detection on the receiver have an ePRI of ~nPRI.

Ammann (NOAA) -- Also requires any PRI difference from one hit to next over any given 4-hit window to maintain a small standard deviation. I'm not sure what exact effect this has. But effectively sets any inter-detection interval to a PRI of 4.8-5.2 (we'll call the average of the 3 lPRI), then applies an additional constraint to keep it all 3 values closeish to lPRI.

UCD1 -- takes several days on several computers for large dataset; data.frame based

UCD data.table pre 2.6 -- TR calculation method has advantages and disadvantages: requires minimal tag drift over time, regardless of temperature or battery life. But it does filter out truly atypical PRIs of tags with many detections for small-hit (3) windows. Having a high flop max mitigates drift in PRI over time.

UCD data.table post 2.6 -- Restores per window, per tag behavior for ePRI selection. However, retains closest to nPRI MODE() tiebreak. Slightly slower than pre 2.6 version. Auto technology recognition of CSV files supported beginning in version 2.7

UCD data.table 3+ -- still in development. Intends to address mimic codes.
