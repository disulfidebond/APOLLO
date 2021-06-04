# simple daemon that scans the NLP directory for new NLP_* files
# in this version, it must be 'reset' weekly

while true ; do
  TS_STRING=$(date +"%m%d%y")
  echo 'waiting for new files...';
  if compgen -G "/project/WEDSS/NLP/NLP_*" >> /dev/null ; then
    echo 'waiting 10 minutes to start copying and begin workflow'
    sleep 600
    # setup log file
    touch logFile_${TS_STRING}.log.txt
    ARR=($(compgen -G "/project/WEDSS/NLP/NLP_*"))
    echo "found files:" >> logFile_${TS_STRING}.log.txt
    V=$(echo "${ARR[0]}" | rev | cut -d/ -f1 | rev)
    V=$(echo "$V" | rev | cut -d_ -f2 | rev)
    for i in "${ARR[@]}" ; do
      echo "$i" | rev | cut -d/ -f1 | rev >> logFile_${TS_STRING}.log.txt
    done

    # create tar.gz archive of weekly report
    # note that in this version, the directory
    # containing the NLP_* files is not removed
    # the directory naming scheme is archiveDir_REPORTDATE_CREATEDDATE
    mkdir archiveDir_${V}_${TS_STRING}
    cp /project/WEDSS/NLP/NLP_* ${PWD}/archiveDir_${V}_${TS_STRING}/
    echo "copied NLP files to archiveDir_${V}_${TS_STRING}" >> logFile_${TS_STRING}.log.txt
    sleep 1;
    tar -czf archiveDir_${V}_${TS_STRING}.tar.gz archiveDir_${V}_${TS_STRING}
    mv archiveDir_${V}_${TS_STRING}.tar.gz /project/WEDSS/NLP/Archive/
    echo "created tar.gz archive of folder archiveDir_${V}_${TS_STRING}" >> logFile_${TS_STRING}.log.txt
    mkdir workdir_${V}_${TS_STRING}
    echo "starting workflow..." >> logFile_${TS_STRING}.log.txt
    sleep 2
    APOLLO_TS=$(date +"%Y-%m-%d")
    OUTDIRNAME=$(date +"%Y%m%d")

    # run APOLLO
    cd /project/WEDSS/NLP/Sandbox/NLPCode_ILM/APOLLO && python APOLLO/ApolloDetector.py --no-final_report_only --report_date "${APOLLO_TS}" --period "trailing_seven_days"
    echo 'copying output files to work directory';
    cp output_dir/${OUTDIRNAME}/final_report* /project/WEDSS/NLP/workdir_${V}_${TS_STRING}/
    mv /project/WEDSS/NLP/NLP_* /project/WEDSS/NLP/workdir_${V}_${TS_STRING}/
    cd /project/WEDSS/NLP/workdir_${V}_${TS_STRING}/
    mv /project/WEDSS/NLP/logFile_${TS_STRING}.log.txt /project/WEDSS/NLP/workdir_${V}_${TS_STRING}/

    # get file names for location mapping preprocessing
    NPATIENT=$(ls $PWD | grep NLP_Patient)
    NSHORT=$(ls $PWD | grep NLP_RiskAndInterventionShortFields)
    NOUTB=$(ls $PWD | grep NLP_Outbreak)
    FNAME=$(ls $PWD | grep final_report | cut -d. -f1)
    FNAMEFILE=$(ls $PWD | grep final_report)

    # delete first row of final_report file
    sed '1d' ${FNAMEFILE} > ${FNAME}.parsed.csv

    # run APOLLO location mapping steps on Silo
    FLLFILE=$(ls $PWD | grep formattedLL)
    python process4location/run_processor.py --data "${NSHORT}" --outbreak "${NOUTB}" --cases "${NPATIENT}"
    python process4location/filter_select.v5_4.py --data "${FNAME}.parsed.csv" --patients "${FLLFILE}"
    break ;
  fi
  sleep 1800 ;
done
echo 'NER Step complete'
