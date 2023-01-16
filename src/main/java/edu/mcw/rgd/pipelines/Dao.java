package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.impl.AnnotationDAO;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * Created by mtutaj on 5/30/2017.
 * <p>
 * All database code lands here
 */
public class Dao {

    AnnotationDAO adao = new AnnotationDAO();
    Logger logInsertAnnots = LogManager.getLogger("insertedAnnots");
    Logger logDeleteAnnots = LogManager.getLogger("deletedAnnots");

    private String version;

    public List<Annotation> getInRgdAnnotations(int createdBy) throws Exception {
        Date cutoffDate = Utils.addDaysToDate(new Date(), 1); // 1 day forward
        String aspect = "S";
        return adao.getAnnotationsModifiedBeforeTimestamp(createdBy, cutoffDate, aspect);
    }

    public void insertAnnot(Annotation annot) throws Exception {

        adao.insertAnnotation(annot);
        logInsertAnnots.debug(annot.dump("|"));
    }

    public void deleteAnnot(Annotation annot) throws Exception {

        adao.deleteAnnotation(annot.getKey());
        logDeleteAnnots.debug(annot.dump("|"));
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
