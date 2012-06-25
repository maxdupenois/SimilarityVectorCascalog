rm SimilarityVectorCascalog-1.0.0-SNAPSHOT*
lein uberjar
scp SimilarityVectorCascalog-1.0.0-SNAPSHOT-standalone.jar deploy@twittermoods.forward.co.uk:max/SimVecCasc.jar
