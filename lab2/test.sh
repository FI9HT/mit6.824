testTimes=100
i=1
logFile="1.log"

> $logFile

while [ $i -le $testTimes ]; do
    echo `date`" => start: "$i
    go test -run 2B >> $logFile
    echo -e "\n\n"

     if grep -q "FAIL" $logFile; then
        echo -e "\n\n=======================YCERROR=================================\n" >> $logFile
     else
        echo -e "\n\n=======================YCOK=================================\n" >> $logFile
     fi
    sleep 1
    ((i++))
done