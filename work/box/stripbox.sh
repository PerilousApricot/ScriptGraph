#!/bin/bash

rm BBBBBBRINGIT
rm dummy.tar.gz
touch SG_DUMMY
tar -cvf dummy.tar.gz SG_DUMMY
SG_FILES_OLD=`ls -1`
echo "files $SG_FILES"
touch BBBBBBRINGIT
SG_FILES_NEW=`ls -1`
echo "files $SG_FILES_NEW"

for DIR_FILE in ${SG_FILES_NEW//:/ }
do

    for SVN_FILE in ${SG_FILES_OLD//::/ }
    do
#        echo "Examining pair ${SVN_FILE} ${DIR_FILE}"
        if [ "${DIR_FILE}" = "dummy.tar.gz" ]
        then
            echo "Don't want to readd the tarball"
            continue 2
        fi

        if [ "${DIR_FILE}" = "${SVN_FILE}" ]
        then
            echo "Appears to be an input file: ${DIR_FILE}"
            continue 2
        fi

    done

    echo "Found a non-input-file, adding ${DIR_FILE}"
    tar -f dummy.tar.gz --append ${DIR_FILE}

done
