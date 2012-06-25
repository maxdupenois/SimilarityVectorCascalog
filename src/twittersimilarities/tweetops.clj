(ns twittersimilarities.tweetops
  (:use cascalog.api)
  (:require
   [clojure.string :as cs]
   [twittersimilarities.utils :as su])
  (:gen-class))


; ******** Map operations ********
(defmapop make-date [year month day]
	(cs/join "-" [year month day])
	)
	
; ******** Filter operations ********
(deffilterop no-repeats [word]
	(nil? (re-find (re-matcher #"(.)\1{2,}" word))))
(deffilterop valid-word [word]
	(nil? (re-find (re-matcher #"^[^a-z]" word))))
(deffilterop valid-length [word]
  (> (.length word) 2))

(deffilterop is-link? [word]
  (re-find #"^http:\/\/t.co/[a-z0-9]{8}$" word))


(deffilterop valid-tweet [timezone]
  (not (nil? (re-find (re-matcher  #"^Eastern Time \(US & Canada\)$" timezone)))))

(defmapop fix-date [date]
  (let [date-parts(seq (.split  date "\\-") )
	year (first date-parts)
	month (su/to-int-fn (second date-parts))
	day (su/to-int-fn (nth date-parts 2))
	]
    (str year "-" (if (< month 10) (identity "0") (identity "")) month "-" (if (< day 10) (identity "0") (identity "")) day)))