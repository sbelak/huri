(ns huri.io
  (:require [huri.core :refer [update-cols]]
            [cheshire.core :as json]            
            [clojure.java.io :as io]))

(defn spit-json
  ([f x]
   (spit-json f {} x))
  ([f {:keys [cast-fns]} x]
   (json/encode-stream (cond->> x
                         cast-fns (update-cols cast-fns))
                       (io/writer f))))

(defn slurp-json
  [f]
  (json/decode-stream (io/reader f) true))
