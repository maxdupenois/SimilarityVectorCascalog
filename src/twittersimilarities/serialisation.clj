(ns twittersimilarities.serialisation
  (:use cascalog.api)
  (:require
   [cascalog.more-taps :as mtaps]
   [cascalog.tap :as tap]
   [cascalog.vars :as v]
   [cascalog.workflow :as w])
  (:import ;; [cascading.scheme WritableSequenceFile]
  	   [cascading.tuple Fields]
  	   [org.apache.hadoop.io Text]
  	   ;; [cascading.flow FlowProps]	
  	   )
   (:gen-class))

(def testdata [
	       ["1" "All"]
	       ["2" "Along"]
	       ["3" "The"]
	       ["4" "Watchtower"]
	       ["5" "Princes"]
	       ["6" "Kept"]
	       ["7" "Their"]
	       ["8" "View"]
	       ])

(defn in-map [key val]
  {key val}
  )
(defn hfs-wrtseqfile
  "Creates a tap on HDFS using sequence file format. Different
   filesystems can be selected by using different prefixes for `path`.

  Supports keyword option for `:outfields`. See `cascalog.tap/hfs-tap`
  for more keyword arguments.

   See http://www.cascading.org/javadoc/cascading/tap/Hfs.html and
   http://www.cascading.org/javadoc/cascading/scheme/SequenceFile.html"
  [path  key-type value-type & opts]
  (let [scheme (-> (:outfields (apply array-map opts) Fields/ALL)
                   (mtaps/writable-sequence-file key-type value-type))]
    (apply tap/hfs-tap scheme path opts))
  )

(defn -main [& args]
    (let [seqtap (hfs-wrtseqfile "/user/maxdupenois/ser-deleteme"  Text  Text :outfields ["?id" "?w"] :sinkmode :replace)]
      (?<- seqtap [?map] (testdata ?id ?w) )
    ))




