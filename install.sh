#!/usr/bin/env bash
/home/mindkicker/idea-IC-141.2735.5/plugins/maven/lib/maven3/bin/mvn clean package
/usr/share/elasticsearch/bin/plugin --remove dbloader
/usr/share/elasticsearch/bin/plugin --url file:///home/mindkicker/IdeaProjects/ElasticDBLoaderPlugin/target/releases/elasticsearch-dbloader-plugin-1.0.zip --install dbloader
sudo service elasticsearch restart