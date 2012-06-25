(ns twittersimilarities.ops
   (:use cascalog.api)
   
   (:gen-class))

; ******** Map operations ********
(defmapop to-int [x](Integer/parseInt x))
	
(defmapop to-double [x] (Double/parseDouble x))

(defmapop extract [cols pattern string](take cols (re-seq pattern string)))

(defmapop sqrt[v] (Math/sqrt v))

(defmapop cosine-sim [dot-prod mag-vector1 mag-vector2] (div dot-prod (* mag-vector1 mag-vector2)))

(defmapop blank-to-zero [n] (if (nil? n) (identity "0.0") (identity n)))

; ******** Combine operations ********
(defmapcatop split [sentence]
	(seq (.split sentence "\\s")))
	
(defn init-magnitude [v] (* v v))
(defparallelagg magnitude-sq :init-var #'init-magnitude :combine-var #'+)


(defn init-dot-product [val1 val2] (* val1 val2))
(defparallelagg dot-product :init-var #'init-dot-product :combine-var #'+)	


(defn combine-pipe-join-strings[current string] (str current "|" string))
(defparallelagg pipe-join-strings :init-var #'identity :combine-var #'combine-pipe-join-strings)

; ******** Reduce operations ********

; ******** Filter operations ********