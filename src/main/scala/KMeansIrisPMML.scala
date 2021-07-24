package pmmlmodel
import org.pmml4s.model.Model

// Snowpark UDFで推論を実行するには不要
class KMeansIrisPMML(modelName: String) {
    def getModel(): Model = {
        val model: Model = Model.fromFile(s"src/main/resources/$modelName")

        model
    }
}
