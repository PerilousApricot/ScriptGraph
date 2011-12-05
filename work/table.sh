#!/usr/bin/env bash

for folder in /uscms/home/meloam/scratch/input39x2/edges/s8solver-*-scale
do
    echo $folder
    line_found=0
    eff_is_found=0
    while read line
    do
        if [[ 0 -eq $line_found ]]
        then
            if [[ 0 -eq $eff_is_found ]]
            then
                if [[ 1 -eq `echo "$line" | awk 'match(\$0, /b\-Tag\ eff.*/){print 1}'` ]]
                then
                    line_found=1
                    continue
                fi
            else
                if [[ 1 -eq `echo "$line" | awk 'match(\$0, /b\-Tag\ scale.*/){print 1}'` ]]
                then
                    line_found=1
                    eff_is_found=1
                    continue
                fi
            fi
        fi

        if [[ 0 -eq $eff_is_found ]]
        then
            if [[ 1 -eq `echo "$line" | awk 'match($0,/bin/){print 1}'` ]]
            then
                new_line=`echo "$line" | sed -e 's/[:()%]//g'`
                effs[${#effs[*]}]=`echo "$new_line" | awk 'match($0, /bin/){print $3}'`
                efferrs[${#efferrs[*]}]=`echo "$new_line" | awk 'match($0, /bin/){print $5}'`
                effrers[${#effrers[*]}]=`echo "$new_line" | awk 'match($0, /bin/){print $6}'`
            else
                line_found=0
                eff_is_found=1

                continue
            fi
        else
            if [[ 1 -eq `echo "$line" | awk 'match($0,/bin/){print 1}'` ]]
            then
                new_line=`echo "$line" | sed -e 's/[:()%]//g'`
                sfs[${#sfs[*]}]=`echo "$new_line" | awk 'match($0, /bin/){print $3}'`
                sferrs[${#sferrs[*]}]=`echo "$new_line" | awk 'match($0, /bin/){print $5}'`
                sfrers[${#sfrers[*]}]=`echo "$new_line" | awk 'match($0, /bin/){print $6}'`
            else
                break
            fi
        fi
    done < $folder/scale.txt

    # Print Table
    bins[${#bins[*]}]="40..50"
    bins[${#bins[*]}]="50..60"
    bins[${#bins[*]}]="60..80"
    bins[${#bins[*]}]="80..140"
    bins[${#bins[*]}]="140.."
    i=0
    size=$((${#bins[@]} - 1))
    let size=size+1
    echo \| *Bin* \| *Efficiency* \| *Eff Err* \| *Eff Rel Err* \| *SF* \| *SF Err* \| *SF Rel Err* \|
    while [[ i -lt $size ]]
    do
        echo \| ${bins[$i]} \| ${effs[$i]} \| ${efferrs[$i]} \| ${effrers[$i]}% \| ${sfs[$i]} \| ${sferrs[$i]} \| ${sfrers[$i]}% \|
        let i=i+1
    done
    echo

    unset bins
    unset sfrers
    unset sferrs
    unset sfs
    unset effrers
    unset efferrs
    unset effs
done

exit 0
