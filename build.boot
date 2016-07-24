(set-env! :source-paths #{"src/clj"}
          :dependencies '[[org.apache.kafka/kafka-clients "0.10.0.0"]
                          [prismatic/schema "1.1.2"]])

(def +version+ "0.1.0-SNAPSHOT")
