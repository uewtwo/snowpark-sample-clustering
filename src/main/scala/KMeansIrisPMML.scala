package pmmlmodel
import org.pmml4s.model.Model

class KMeansIrisPMML(modelName: String) {
    def getModel(): Model = {
        val model: Model = Model.fromFile(s"src/main/resources/$modelName")

        model
    }
}
