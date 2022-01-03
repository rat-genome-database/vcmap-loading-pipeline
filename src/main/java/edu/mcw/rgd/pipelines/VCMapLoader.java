package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;


/**
 * Created by mtutaj on 5/30/2017
 */
public class VCMapLoader {

    private String version;
    private int nr;
    private Dao dao;

    Logger log = LogManager.getLogger("status");

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        VCMapLoader manager = (VCMapLoader) (bf.getBean("manager"));

        try {
            manager.run();
        } catch(Exception e) {
            Utils.printStackTrace(e, manager.log);
            throw e;
        }
    }

    public void run() throws Exception {

        long time0 = System.currentTimeMillis();

        log.info(getVersion());

        // todo

        String msg = "=== OK === elapsed "+ Utils.formatElapsedTime(time0, System.currentTimeMillis());
        log.info(msg);
    }



    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public int getNr() {
        return nr;
    }

    public void setNr(int nr) {
        this.nr = nr;
    }

    public Dao getDao() {
        return dao;
    }

    public void setDao(Dao dao) {
        this.dao = dao;
    }
}
