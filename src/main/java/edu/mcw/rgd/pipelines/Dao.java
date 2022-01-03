package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.AnnotationDAO;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
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



    public List<Annotation> getQtlRsoAnnotationsNotCreatedByPipeline(int createdBy) throws Exception {
        String sql = "select * from FULL_ANNOT fa\n" +
                "WHERE fa.rgd_object_key=6 AND fa.aspect='S' AND fa.created_by <> ?";
        return adao.executeAnnotationQuery(sql, createdBy);
    }

    public List<Annotation> getInRgdAnnotations(int createdBy) throws Exception {
        Date cutoffDate = Utils.addDaysToDate(new Date(), 1); // 1 day forward
        String aspect = "S";
        return adao.getAnnotationsModifiedBeforeTimestamp(createdBy, cutoffDate, aspect);
    }

    public List<Annotation> getIncomingAnnotations(int createdBy) throws Exception {

        String sql =
                " SELECT DISTINCT ot.TERM AS term,\n" +
                "    qtls.rgd_id             AS rgd_id,\n" +
                "    qtls.qtl_symbol         AS symbol,\n" +
                "    fa1.ref_rgd_id          AS ref,\n" +
                "    qtls.qtl_name           AS name,\n" +
                "    ot.term_acc             AS term_acc\n" +
                "  FROM ONT_TERMS ot,\n" +
                "    ONT_SYNONYMS os,\n" +
                "    strains st,\n" +
                "    rgd_qtl_strain rqs,\n" +
                "    qtls,\n" +
                "    full_annot fa1,\n" +
                "    rgd_ids\n" +
                "  WHERE st.strain_key = rqs.strain_key\n" +
                "  AND rqs.qtl_key     = qtls.qtl_key\n" +
                "  AND os.SYNONYM_NAME LIKE 'RGD ID:%'\n" +
                "  AND to_number(SUBSTR(os.SYNONYM_NAME,9, 100)) = st.RGD_ID\n" +
                "  AND ot.TERM_ACC                               = os.TERM_ACC\n" +
                "  AND qtls.rgd_id                               = rgd_ids.rgd_id\n" +
                "  AND rgd_ids.OBJECT_STATUS                     = 'ACTIVE'\n" +
                "  AND qtls.rgd_id                               = fa1.annotated_object_rgd_id\n" +
                "  AND fa1.aspect                                = 'L'";

        List<Annotation> annots = new ArrayList<Annotation>();

        try( Connection conn = DataSourceFactory.getInstance().getDataSource().getConnection() ) {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while( rs.next() ) {
                String termName = rs.getString(1);
                int qtlRgdId = rs.getInt(2);
                String qtlSymbol = rs.getString(3);
                int refRgdId = rs.getInt(4);
                String qtlName = rs.getString(5);
                String termAcc = rs.getString(6);

                Annotation a = new Annotation();
                a.setTerm(termName);
                a.setAnnotatedObjectRgdId(qtlRgdId);
                a.setRgdObjectKey(6);
                a.setDataSrc("RGD");
                a.setObjectSymbol(qtlSymbol);
                a.setRefRgdId(refRgdId);
                a.setEvidence("IEA");
                a.setAspect("S");
                a.setObjectName(qtlName);
                a.setTermAcc(termAcc);
                a.setCreatedBy(createdBy);
                a.setLastModifiedBy(createdBy);

                annots.add(a);
            }
        }
        return annots;
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
