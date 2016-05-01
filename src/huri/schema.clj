(ns huri.schema
  (:require [plumbing.fnk.schema :refer [assert-iae]]  
            (schema [coerce :as s.coerce]
                    [core :as s])))

(def coercers (atom {}))

(defn register-coercer
  [schema coercer]
  (swap! coercers assoc (:name (meta schema)) coercer))

(defn coerce
  ([schema]
   (s.coerce/coercer! schema (comp @coercers :name meta)))
  ([schema x]
   ((coerce schema) x)))

(defmacro defcoercer
  [name schema [& args] & body]
  `(do
     (assert-iae (-> ~schema meta :name) "%s is not a named schema" ~schema)
     (register-coercer ~schema (fn [~@args] ~@body))
     (def ~name (coerce ~schema))))

(s/defschema IFn (s/pred ifn?))
(s/defschema Coll (s/maybe (s/pred coll?)))
(s/defschema Map {s/Any s/Any})

(defn AlwaysSeq
  [& s]
  (s/schema-with-name (vec s) 'AlwaysSeq))

(defcoercer ensure-seq (AlwaysSeq s/Any)
  [x]
  (if (sequential? x)
    x
    [x]))

