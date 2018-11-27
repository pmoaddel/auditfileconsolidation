import org.apache.orc.TypeDescription;

public class AuditSchema {
    public static TypeDescription getSchema() {
        return TypeDescription.createStruct()
            .addField("org_id", TypeDescription.createString())
            .addField("user_id", TypeDescription.createString())
            .addField("user_name", TypeDescription.createString())
            .addField("id", TypeDescription.createString());
    }
}
