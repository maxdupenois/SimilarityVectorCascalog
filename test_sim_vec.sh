#lein compile
lein uberjar
hadoop --config ../hadoop-remote-conf/ fs -rmr "/tmp/max/twitter_tmp/"
hadoop --config ../hadoop-remote-conf/ jar SimilarityVectorCascalog-1.0.0-SNAPSHOT-standalone.jar SimilarityVectorCascalog.core "2012?0{1,2,3,4,5,6}*" "201201-06"

#"/user/hive/warehouse/twitter_moods/dated=2012-06-*"