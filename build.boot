(set-env! :source-paths #{"src"}
          :dependencies '[[org.apache.kafka/kafka-clients "0.10.0.0" :scope "provided"]
                          [prismatic/schema "1.1.2" :scope "provided"]
                          [adzerk/bootlaces "0.1.13" :scope "test"]
                          [adzerk/boot-test "1.1.1" :scope "test"]])

(require '[adzerk.boot-test :as test]
         '[adzerk.bootlaces :refer :all])

(def +version+ "0.1.0-SNAPSHOT")
(bootlaces! +version+ :dont-modify-paths? true)

(task-options!
 pom {:project 'io.thdr/kfk
      :version +version+
      :description "Thin Kafka Java API (0.9 - 0.10) wrapper for Clojure"
      :url "https://github.com/konukhov/kfk"
      :scm {:url "https://github.com/konukhov/kfk"}
      :license {"Eclipse Public License"
                "http://www.eclipse.org/legal/epl-v10.html"}})

(deftask test []
  (merge-env! :source-paths #{"test"})
  (test/test))
