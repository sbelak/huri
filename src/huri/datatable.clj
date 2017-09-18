(ns huri.datatable
  (:require [huri.core]
            [hiccup.element]
            [gorilla-repl.html]))

(defn df->hiccup
  "Transforms a dataframe into hiccup, which in a browser renders the data with JQuery datatables"
  ([df]
   (df->hiccup {} df))
  ([opts df]        
   (let [{:keys [limit sort-by pagination? header-column? columns filtering?]
          :or {limit 5
               pagination? true
               filtering? false}} opts
         id (name (gensym "df"))
         headers (zipmap (huri.core/cols df) (range))         
         order (let [[sort-col sort-direction] (-> sort-by
                                                   (or (ffirst headers))
                                                   (huri.core/ensure-seq))]
                 (format "order: [[%s, '%s']]" (plumbing.core/safe-get headers sort-col)
                         (name (or sort-direction :asc))))
         pagination (format "bPaginate: %s" pagination?)
         info (format "info: %s" pagination?)
         filtering (format "bFilter: %s" filtering?)
         dt-options (clojure.string/join "," [order pagination info filtering])
         columns (or columns (keys headers))]
     (->> (list [:table.stripe.hover.cell-border {:id id}
                 [:thead
                  [:tr (for [h columns]
                         [:th (name h)])]]
                 [:tbody (for [row (if (= limit :all)
                                     df
                                     (take limit df))]
                           [:tr (map-indexed (fn [i cell]
                                               [(if (and header-column?
                                                         (zero? i))
                                                  :th
                                                  :td) cell])
                                             ((apply juxt columns) row))])]]
                (hiccup.element/javascript-tag (format "$('#%s').DataTable({%s});"
                                                       id dt-options)))
          ))))


(defn datatable
  "renders a dataframe with datatables in gorilla repl"
  ([df]
   (datatable {} df))
  ([opts df]
   (->> df
        (hiccup.core/html)
        (df->hiccup)
        (gorilla-repl.html/html-view)
        
        ))
  )
