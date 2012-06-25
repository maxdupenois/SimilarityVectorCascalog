(defproject SimilarityVectorCascalog "1.0.0-SNAPSHOT"
  :source-path "src"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [cascalog "1.8.7" :exclusions [org.slf4j/slf4j-log4j12]]
                 [cascalog-checkpoint "0.1.1" :exclusions [cascalog]]
		 [hadoop-util "0.2.7" :exclusions [org.slf4j/slf4j-log4j12]]	
		 [org.slf4j/slf4j-simple "1.6.4"]
		 [enlive "1.0.0-SNAPSHOT"]
                 [cascalog-more-taps "0.2.1"]
		 [org.pingles/cascading.protobuf "0.0.1"]
		 ;; [cascading/cascading-core "1.2.4" :only [cascading.scheme/WritableSequenceFile]
		;; [cascading/cascading-hadoop "2.0.0"
		;;  :exclusions [org.codehaus.janino/janino
		;; 	      org.apache.hadoop/hadoop-core]]
		;; [cascading/cascading-core "1.2.4" :only [cascading.scheme/WritableSequenceFile]]
		]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev" :exclusions [org.slf4j/slf4j-api]]]
  :aot [twittersimilarities.core twittersimilarities.tweetnewsvectors twittersimilarities.serialisation])

  ; :description "Will hopefully build a similarity vector for subsequent days from the twitter feed"	
  ;   :repositories {"conjars.org" "http://conjars.org/repo"
  ;   				"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"}
  ;   :disable-implicit-clean true
  ;   :jvm-opts ["-Xmx1g" "-server"]
  ;   :resources-path "./resources"
  ;   :proto-path "vendor/uswitch-protocol-buffers/schema"
  ;   :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-cdh3u2" :exclusions [org.slf4j/slf4j-api]]
  ; 	                     [midje-cascalog "0.4.0"]
  ; 	                     [protobuf "0.6.0-beta14"]                     
  ; 	                     [org.slf4j/slf4j-simple "1.6.4"]]
  ;   :dependencies [[org.clojure/clojure "1.3.0"]
  ;                [org.clojure/tools.cli "0.2.1"]
  ;                [clj-time "0.4.0"]
  ;                [cascalog "1.8.7"]
  ;                [cascalog-more-taps "0.2.0"]
  ;                [cascalog-checkpoint "0.1.1" :exclusions [cascalog]]
  ;                [com.google.protobuf/protobuf-java "2.3.0"]
  ;                [bytebuffer "0.2.0"]                 
  ;                [mysql/mysql-connector-java "5.1.19"]
  ;                [r0man/cascading.jdbc "1.2"]])