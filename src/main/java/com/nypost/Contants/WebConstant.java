package com.nypost.Contants;

import java.util.regex.Pattern;

public class WebConstant {

    public final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|mp3|mp4|zip|gz))$");
    public static final String CRAWL_REPORT_NYPOST = "/Users/mukesh/Office/crawler/data/crawl/CrawlReport_nypost.txt";


    public static final String URL = "URL";
    public static final String CONTENT_SIZE = "Content Size";
    public static final String OUTGOING_URL_NUMBER = "No of Outgoing Link";
    public static final String CONTENT_TYPE = "Content Type";
    public static final String STATUS_CODE = "STATUS_CODE";
    public static final String STATUS_OK_NOK = "OK/N_OK";
    public static final String STATUS_OK = "OK";
    public static final String STATUS_N_OK = "N_OK";


    public static final String FETCH_NYPOST = "data/crawl/fetch_nypost.csv";
    public static final String VISIT_NYPOST = "data/crawl/visit_nypost.csv";
    public static final String URL_NYPOST = "data/crawl/url_nypost.csv";

    public static final String OK_NOVISIT_NYPOST = "data/crawl/ok_novisit_nypost.csv";


}
