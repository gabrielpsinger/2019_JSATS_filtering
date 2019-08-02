### From M. Pagel 2019-01-23
### After the filtering code has been run.  Combine the files from each technology for import into the ACCESS db

# In Git Bash alter and copy-paste as appropriate

cd /c/file/path/to/directory/named/accepted/ 
# you can also do the above as cd c:\file\path\to\directory\named\accepted in Git Bash as per windows style. Do not do this in R

echo 'Dec,Hex,SigStr,RecSN,dtf,fullHex,nPRI,FracSec' > LOTcombo.csv
head -n1 *15-6031_accepted.csv > SUMcombo.csv
echo 'Internal,SiteName,SiteName2,SiteName3,dtf,Hex,Tilt,VBatt,Temp,Pres,SigStr,BitPeriod,Thresh,Detection,RecSN,fullHex, nPRI,FracSec' > ATScombo.csv
cat `ls -1 | grep "[0-9]\{4\}_[0-9]\{5\}_accepted\.csv$"` | grep -v 'RecSN' >> ATScombo.csv
cat `ls -1 | grep "^[0-9]\{4\}_1[5678]-6[0-9]\{3\}_accepted\.csv$"` | grep -v 'Detection' >> SUMcombo.csv
cat `ls -1 | grep "^[0-9]\{4\}_\(20[1-2][0-9]\)\?00[0-9]\{3\}_accepted\.csv$"` | grep -v 'Dec' >> LOTcombo.csv

# *If there is no 15-6031 in tekno, you may have to alter the head -n1 command
# **Lotek (last line) may be off as per our alterations to the R script...but I think I altered it properly, assuming the data has the same columns as indicated
