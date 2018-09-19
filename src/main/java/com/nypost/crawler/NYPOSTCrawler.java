package com.nypost.crawler;

import com.nypost.Contants.WebConstant;
import com.nypost.Utils.CSVUtils;
import com.nypost.pojo.FetechNYPost;
import com.nypost.pojo.NYPostCrawlInfo;
import com.nypost.pojo.URLsNYPost;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class NYPOSTCrawler extends WebCrawler {
    final static Logger logger = Logger.getLogger(Controller.class);

    static List<NYPostCrawlInfo> visitNyPostData = new ArrayList<>();
    static List<FetechNYPost> fetchNyPostData = new ArrayList<>();
    static List<URLsNYPost> urlsNyPostData = new ArrayList<>();

    private void addURLsNYPost(WebURL curURL) {
        try {
            String href = curURL.getURL().toLowerCase();
            String status = (href.startsWith(Controller.HTTPS_NY_POST_NEWS) ||
                    href.startsWith(Controller.HTTP_NY_POST_NEWS)) ? WebConstant.STATUS_OK : WebConstant.STATUS_N_OK;
            URLsNYPost urlsNyPost = URLsNYPost.builder().url(curURL.getURL()).okayStatus(status).isExcluded(WebConstant.FILTERS.matcher(href).matches()).build();

            urlsNyPostData.add(urlsNyPost);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private boolean shouldReturnIndicator(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        return !WebConstant.FILTERS.matcher(href).matches()
                && (href.startsWith(Controller.HTTPS_NY_POST_NEWS) || href.startsWith(Controller.HTTP_NY_POST_NEWS));
    }

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        addURLsNYPost(url);
        return shouldReturnIndicator(referringPage, url);
    }


    private void updateVisitStats(Page page) {
        String url = page.getWebURL().getURL();
        String contentType = page.getContentType();
        if (contentType.contains(WebConstant.TEXT_HTML)) {
            contentType = WebConstant.TEXT_HTML;
        }
        HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();

        Set<WebURL> outGoingLinks = htmlParseData.getOutgoingUrls();

        NYPostCrawlInfo nyPostCrawlInfo = NYPostCrawlInfo.builder().url(url.replaceAll(",", "_")).statusCode(String.valueOf(page.getStatusCode())).
                contentType(contentType).outLinkNumbers(outGoingLinks.size()).
                contentSize(page.getContentData().length / 1024)
                .build();
        visitNyPostData.add(nyPostCrawlInfo);
    }


    @Override
    public void visit(Page page) {
        updateVisitStats(page);
    }

    @Override
    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
        FetechNYPost fetchData = FetechNYPost.builder().url(webUrl.getURL().replaceAll(",", "_")).statusCode(String.valueOf(statusCode)).build();
        fetchNyPostData.add(fetchData);

    }


    public static void onBeforeExitCrawler() {
        try {
            FileWriter writer = new FileWriter(WebConstant.FETCH_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL, WebConstant.STATUS_CODE));
            buildFetchCSV(writer);


            writer = new FileWriter(WebConstant.VISIT_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL, WebConstant.CONTENT_SIZE, WebConstant.OUTGOING_URL_NUMBER, WebConstant.CONTENT_TYPE));
            buildVisitCSV(writer);


            writer = new FileWriter(WebConstant.URL_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL, WebConstant.STATUS_OK_NOK));
            buildURLCSV(writer);

            writer = new FileWriter(WebConstant.OK_NOVISIT_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL));

            StringBuffer sbuff = new StringBuffer();
            sbuff.append(WebConstant.AUTH_NAME).append(WebConstant.AUTH_ID).append(WebConstant.CRAWL_WEB).append(WebConstant.SEG_LINE).
                    append(WebConstant.FETCH_STAT).append(WebConstant.SEG_LINE);


            long featchSucceded = 0;
            long twoHund = 0, threeHundOne = 0, fourNotOne = 0, fourNotThree = 0, fourNotFour = 0;


            for (int idx = 0; idx < fetchNyPostData.size(); idx++) {
                FetechNYPost data = fetchNyPostData.get(idx);
                int statusCode = Integer.valueOf(data.getStatusCode());

                if (statusCode >= 200 && statusCode <= 299) featchSucceded++;
                if (statusCode == 200) twoHund++;
                if (statusCode == 301) threeHundOne++;
                if (statusCode == 401) fourNotOne++;
                if (statusCode == 403) fourNotThree++;
                if (statusCode == 404) fourNotFour++;


            }

            int htmlCount = 0, imageGifCount = 0, imageJPEGCount = 0, imagePNGCount = 0, pdfCount = 0;
            int lessOneKB = 0, oneToTenKB = 0, tenToHunKB = 0, hundToOneMB = 0, moreThanOneMB = 0;

            for (int idx = 0; idx < visitNyPostData.size(); idx++) {

                NYPostCrawlInfo data = visitNyPostData.get(idx);
                if (data.getContentType().contains(WebConstant.TEXT_HTML)) htmlCount++;
                if (data.getContentType().contains(WebConstant.IMAGE_GIF)) imageGifCount++;
                if (data.getContentType().contains(WebConstant.IMAGE_JPEG)) imageJPEGCount++;
                if (data.getContentType().contains(WebConstant.IMAGE_PNG)) imagePNGCount++;
                if (data.getContentType().contains(WebConstant.APP_PDF)) pdfCount++;

                if (data.getContentSize() < 1) lessOneKB++;
                if (data.getContentSize() >= 1 && data.getContentSize() < 10) oneToTenKB++;
                if (data.getContentSize() >= 10 && data.getContentSize() < 100) tenToHunKB++;
                if (data.getContentSize() >= 100 && data.getContentSize() < 1024) hundToOneMB++;
                else moreThanOneMB++;


            }

            //long totalOutGoingNumbers = visitNyPostData.parallelStream().mapToInt(data -> data.getOutLinkNumbers()).sum();


            long OKCount = urlsNyPostData.parallelStream().filter(data -> data.getOkayStatus().equals(WebConstant.STATUS_OK)).distinct().count();
            long NOKCount = urlsNyPostData.parallelStream().filter(data -> data.getOkayStatus().equals(WebConstant.STATUS_N_OK)).distinct().count();

            sbuff.append(WebConstant.FETCH_ATEMP + fetchNyPostData.size()).append(WebConstant.FETEH_SCUS + featchSucceded);
            sbuff.append(WebConstant.FETCH_ABORT_FAIL + (fetchNyPostData.size() - featchSucceded));

            sbuff.append(WebConstant.OUTGOING_URL);
            sbuff.append(WebConstant.SEG_LINE).
                    append(WebConstant.TOTAL_URL_EXT + fetchNyPostData.size());
            sbuff.append(WebConstant.TOTAL_UNQ_URL_EXT + fetchNyPostData.parallelStream().distinct().count());
            sbuff.append(WebConstant.TOTAL_UNQ_URL_INSIDE + OKCount);
            sbuff.append(WebConstant.TOTAL_UNQ_URL_OUTSIDE + NOKCount);


            sbuff.append(WebConstant.STATUS_CODES);
            sbuff.append(WebConstant.SEG_LINE);
            sbuff.append(WebConstant.TWO_100 + twoHund);
            sbuff.append(WebConstant.THREE_101 + threeHundOne);
            sbuff.append(WebConstant.FOUR_101 + fourNotOne);
            sbuff.append(WebConstant.FOUR_103 + fourNotThree);
            sbuff.append(WebConstant.FOUR_104 + fourNotFour);

            sbuff.append(WebConstant.FILE_SIZES).append(WebConstant.SEG_LINE);

            sbuff.append(WebConstant.LESS_1).append(lessOneKB);
            sbuff.append(WebConstant.ONE_TEN_KB).append(oneToTenKB);
            sbuff.append(WebConstant.TEN_HUND_KB).append(tenToHunKB);
            sbuff.append(WebConstant.HUND_HUND_ONE_MB).append(hundToOneMB);
            sbuff.append(WebConstant.MORE_THANN_ONE_MB).append(moreThanOneMB);


            sbuff.append(WebConstant.CONTENT_TYPE_STR);
            sbuff.append(WebConstant.SEG_LINE);
            sbuff.append(WebConstant.TEXT_HTML + htmlCount);
            sbuff.append(WebConstant.IMAGE_GIF + imageGifCount);
            sbuff.append(WebConstant.IMAGE_JPEG + imageJPEGCount);
            sbuff.append(WebConstant.IMAGE_PNG + imagePNGCount);
            sbuff.append(WebConstant.APP_PDF + pdfCount);


            buildCrawlReportNYPosy(sbuff.toString());
        } catch (Exception e) {
            e.printStackTrace();

        }
    }


    private static void buildCrawlReportNYPosy(String content) {
        try {
            PrintWriter writer = new PrintWriter(new File(WebConstant.CRAWL_REPORT_NYPOST), "UTF-8");
            writer.print(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    private static void buildURLCSV(FileWriter writer) {
        try {
            urlsNyPostData.parallelStream().forEach(data -> {
                try {
                    CSVUtils.writeLine(writer, Arrays.asList(data.getUrl(), String.valueOf(data.getOkayStatus())));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("urlsNyPostData " + urlsNyPostData.size());
            writer.flush();
            writer.close();
        } catch (Exception e) {

        }
    }

    private static void buildVisitCSV(FileWriter writer) {
        try {
            visitNyPostData.parallelStream().forEach(data -> {
                try {
                    CSVUtils.writeLine(writer, Arrays.asList(data.getUrl(), String.valueOf(data.getContentSize()), String.valueOf(data.getOutLinkNumbers()), data.getContentType()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("NyPostData " + visitNyPostData.size());
            writer.flush();
            writer.close();
        } catch (Exception e) {

        }
    }


    public static void buildFetchCSV(FileWriter writer) {
        try {
            fetchNyPostData.parallelStream().forEach(data -> {
                try {
                    CSVUtils.writeLine(writer, Arrays.asList(data.getUrl(), data.getStatusCode()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("fetchNyPostData " + fetchNyPostData.size());
            writer.flush();
            writer.close();
        } catch (Exception e) {

        }
    }
}
