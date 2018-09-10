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
    long featchSucceded = 0;
    static List<String> okNoVisitNyPostData = new ArrayList<>();


    private void handleUrl(WebURL curURL) {
        try {
            int statusCode  =200;// this.getMyController().getPageFetcher().fetchPage(curURL).getStatusCode();

            String href = curURL.getURL().toLowerCase();
            String status = (href.startsWith(Controller.HTTPS_NY_POST_NEWS) ||
                    href.startsWith(Controller.HTTP_NY_POST_NEWS)) ? WebConstant.STATUS_OK : WebConstant.STATUS_N_OK;
            URLsNYPost urlsNyPost = URLsNYPost.builder().url(curURL.getURL()).okayStatus(status).isExcluded(WebConstant.FILTERS.matcher(href).matches()).statusCode(statusCode).build();

            urlsNyPostData.add(urlsNyPost);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        String href = url.getURL().toLowerCase();
        handleUrl(url);
        return !WebConstant.FILTERS.matcher(href).matches()
                && href.startsWith(Controller.HTTPS_NY_POST_NEWS);
    }

    @Override
    public void visit(Page page) {

        String url = page.getWebURL().getURL();

        if (page.getParseData() instanceof HtmlParseData) {
            String contentType = "text/html";
            try {
                contentType = page.getContentType().substring(0, page.getContentType().indexOf(";"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            int statusCode = page.getStatusCode();
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();

            String html = htmlParseData.getHtml();
            Set<WebURL> outGoingLinks = htmlParseData.getOutgoingUrls();

            NYPostCrawlInfo nyPostCrawlInfo = NYPostCrawlInfo.builder().url(url).statusCode(String.valueOf(statusCode)).
                    contentType(contentType).outLinkNumbers(outGoingLinks.size()).
                    contentSize(html.length() / 1024)
                    .build();
            visitNyPostData.add(nyPostCrawlInfo);
        } else {
            okNoVisitNyPostData.add(url);
        }
    }

    @Override
    protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
        FetechNYPost fetchData = FetechNYPost.builder().url(webUrl.getURL()).statusCode(String.valueOf(statusCode)).build();
        fetchNyPostData.add(fetchData);

    }

    @Override
    public void onBeforeExit() {
        try {
            FileWriter writer = new FileWriter(WebConstant.FETCH_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL, WebConstant.STATUS_CODE));
            buildFetchCSV(writer);


            writer = new FileWriter(WebConstant.VISIT_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL, WebConstant.CONTENT_SIZE, WebConstant.OUTGOING_URL_NUMBER, WebConstant.CONTENT_TYPE));
            buildVisitCSV(writer);


            writer = new FileWriter(WebConstant.URL_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL, WebConstant.STATUS_OK_NOK, "HREF","STATUS Code"));
            buildURLCSV(writer);

            writer = new FileWriter(WebConstant.OK_NOVISIT_NYPOST);
            CSVUtils.writeLine(writer, Arrays.asList(WebConstant.URL));
            buildOKVisitURLCSV(writer);

            StringBuffer sbuff = new StringBuffer();
            sbuff.append("Name :\t Mukesh Dangi \n");
            sbuff.append("\n USC ID :\t 4297380684");
            sbuff.append("\n News site crawled: nypost.com");

            sbuff.append("\n\n ========================================================");
            sbuff.append("\n\t\tFetch Statistics");
            sbuff.append("\n ========================================================");


            sbuff.append("\n# fetches attempted:\t" + visitNyPostData.size());
            featchSucceded = visitNyPostData.parallelStream().filter(data -> !(Integer.valueOf(data.getStatusCode()) < 200 || Integer.valueOf(data.getStatusCode()) > 299)).count();

            sbuff.append("\n# fetches succeeded:\t" + featchSucceded);
            sbuff.append("\n# fetches Failed/Aborted:\t" + (visitNyPostData.size() - featchSucceded));

            long twoHund = visitNyPostData.parallelStream().filter(data -> (Integer.valueOf(data.getStatusCode()) == 200)).count();
            long threeHundOne = visitNyPostData.parallelStream().filter(data -> (Integer.valueOf(data.getStatusCode()) == 301)).count();
            long fourNotOne = visitNyPostData.parallelStream().filter(data -> (Integer.valueOf(data.getStatusCode()) == 401)).count();
            long fourNotThree = visitNyPostData.parallelStream().filter(data -> (Integer.valueOf(data.getStatusCode()) == 403)).count();
            long fourNotFour = visitNyPostData.parallelStream().filter(data -> (Integer.valueOf(data.getStatusCode()) == 404)).count();

            long textHtml = visitNyPostData.parallelStream().filter(data -> data.getContentType().contains("text/html")).count();
            long imageGif = visitNyPostData.parallelStream().filter(data -> data.getContentType().contains("image/gif")).count();
            long imageJpeg = visitNyPostData.parallelStream().filter(data -> data.getContentType().contains("image/jpeg")).count();
            long imagePng = visitNyPostData.parallelStream().filter(data -> data.getContentType().contains("image/png")).count();
            long pdfCount = visitNyPostData.parallelStream().filter(data -> data.getContentType().contains("application/pdf")).count();

            long totalOutGoingNumbers = visitNyPostData.parallelStream().mapToInt(data -> data.getOutLinkNumbers()).sum();

            long OKCount = urlsNyPostData.parallelStream().filter(data -> data.getOkayStatus().equals(WebConstant.STATUS_OK)).count();
            long NOKCount = urlsNyPostData.parallelStream().filter(data -> data.getOkayStatus().equals(WebConstant.STATUS_N_OK)).count();
            sbuff.append("\n\nOutgoing URLs:");
            sbuff.append("\n=============");
            sbuff.append("\n # Unique URLs extracted:\t" + totalOutGoingNumbers);
            sbuff.append("\n # Unique URLs within News Site:\t" + OKCount);
            sbuff.append("\n # Unique URLs outside News Site:\t" + NOKCount);


            sbuff.append("\n\nStatus Codes:");
            sbuff.append("\n=============");
            sbuff.append("\n 200 OK:\t" + twoHund);
            sbuff.append("\n 301 Moved Permanently:\t" + threeHundOne);
            sbuff.append("\n 401 Unauthorized:\t" + fourNotOne);
            sbuff.append("\n 403 Forbidden:\t" + fourNotThree);
            sbuff.append("\n 404 Not Found:\t" + fourNotFour);

            sbuff.append("\n\n Content Types: ");
            sbuff.append("\n=============");
            sbuff.append("\n text/html:\t" + textHtml);
            sbuff.append("\n image/gif:\t" + imageGif);
            sbuff.append("\n image/jpeg:\t" + imageJpeg);
            sbuff.append("\n image/png:\t" + imagePng);
            sbuff.append("\n application/pdf:\t" + pdfCount);


            buildCrawlReportNYPosy(sbuff.toString());
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    private void buildOKVisitURLCSV(FileWriter writer) {
        try {
            okNoVisitNyPostData.parallelStream().forEach(data -> {
                try {
                    CSVUtils.writeLine(writer, Arrays.asList(data));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            writer.flush();
            writer.close();
        } catch (Exception e) {

        }

    }

    private void buildCrawlReportNYPosy(String content) {
        try {
            PrintWriter writer = new PrintWriter(new File(WebConstant.CRAWL_REPORT_NYPOST), "UTF-8");
            writer.print(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    private void buildURLCSV(FileWriter writer) {
        try {
            urlsNyPostData.parallelStream().forEach(data -> {
                try {
                    CSVUtils.writeLine(writer, Arrays.asList(data.getUrl(), String.valueOf(data.getOkayStatus()), String.valueOf(data.isExcluded()), String.valueOf(data.getStatusCode())));
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
