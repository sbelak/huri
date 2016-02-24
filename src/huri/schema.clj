(ns huri.schema
  (:require [plumbing.core :refer [map-from-keys]]
            [plumbing.fnk.schema :refer [assert-iae]]  
            [schema.coerce :as s.coerce]))

(def schema-set (comp set
                      (partial keep (comp :name meta))
                      (partial tree-seq coll? seq)))

(def coercers (atom {}))

(defn register-coercer
  [schema coercer]
  (swap! coercers assoc (:name (meta schema)) coercer))

(defmacro defcoercer
  [name schema [& args] & body]
  `(do
     (assert-iae (-> ~schema meta :name) "%s is not a named schema" ~schema)
     (register-coercer ~schema (fn [~@args] ~@body))
     (def ~name (s.coerce/coercer ~schema (comp (->> ~schema
                                                     schema-set
                                                     (map-from-keys @coercers))
                                                :name
                                                meta)))))
