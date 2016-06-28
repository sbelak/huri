(ns huri.etl
  (:require (plumbing [graph :as graph]
                      [core :refer [defnk]]
                      [map :refer [map-leaves-and-path keep-leaves
                                   safe-select-keys]])
            (clj-time [core :as t]
                      [periodic :as t.periodic])
            [plumbing.fnk.pfnk :as pfnk]
            [taoensso.timbre :as log]))

(def task-graph (atom {}))

(def register-task (partial swap! task-graph assoc))

(defmacro deftask 
  [task [& args] & body]
  `(do
     (defnk ~task [~@args] ~@body)
     (register-task ~(keyword task)
                    (vary-meta ~task (partial merge (meta (var ~task)))))))

(def exception? (partial instance? Exception))

(def with-error-handler 
  (partial map-leaves-and-path
           (fn [ks f]
             (pfnk/fn->fnk
               (fn [m]
                 (try
                   (if (some (comp exception? val) m)
                     (throw (ex-info "Upstream error" {}))
                     (f m))
                   (catch Exception e
                     (log/error e ks)
                     e)))
               [(pfnk/input-schema f)
                (pfnk/output-schema f)]))))

(defn run
  [& {:keys [execution-strategy] :or {execution-strategy :parallel}}]
  (into {} (((case execution-strategy
               :parallel graph/par-compile
               :sequential graph/compile)
             (with-error-handler @task-graph)) {})))

(defn run-only
  [tasks & {:keys [evaluation-strategy] :or {evaluation-strategy :parallel}}]
  (safe-select-keys ((graph/lazy-compile (with-error-handler @task-graph)) {})
                     tasks))

(defn run-if
  [pred]
  (run (keys (keep-leaves pred @task-graph))))

(defn refreshing
  [at period f]
  (let [cache (atom ::empty)
        schedule (atom (t.periodic/periodic-seq at period))]
    (reify clojure.lang.IDeref
      (deref [_]
        (if (or (t/after? (t/now) (first @schedule)) (= @cache ::empty))
          (do
            (swap! schedule (partial drop-while (partial t/after? (t/now))))
            (reset! cache nil)
            (reset! cache (f)))
          @cache)))))
