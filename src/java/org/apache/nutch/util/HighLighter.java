package org.apache.nutch.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.highlight.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangmingke on 16/6/24.
 */
public class HighLighter {
    public String HighLight(String query, String text, Analyzer analyzer) throws IOException {
        //SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer();

        TokenStream tokenStreamKeyword = analyzer.tokenStream("field", query);
        tokenStreamKeyword.reset();
        tokenStreamKeyword.addAttribute(CharTermAttribute.class);
        List<String> list = new ArrayList<String>();

        while (tokenStreamKeyword.incrementToken()){
            list.add(tokenStreamKeyword
                    .getAttribute(CharTermAttribute.class).toString());
        }
        tokenStreamKeyword.close();

        SimpleHTMLFormatter simpleHtmlFormatter = new SimpleHTMLFormatter("<font color=\"RED\">","</font>");
        //int i = 0;
        int length = list.size();
        String end = null;

        for(int i = 0; i < length; i ++){
            if(text.indexOf(list.get(i)) != -1) {
                Term term = new Term("field", list.get(i));
                Query query1 = new TermQuery(term);

                Highlighter highlighter = new Highlighter(simpleHtmlFormatter, new QueryScorer(query1));
                highlighter.setTextFragmenter(new SimpleFragmenter(Integer.MAX_VALUE));
                try {
                    TokenStream tokenStream = analyzer.tokenStream("field", text);
                    end = highlighter.getBestFragment(tokenStream, text);
                    text = end;
                } catch (InvalidTokenOffsetsException e) {
                    e.printStackTrace();
                }
            }
        }
        //System.out.println(text);
        return text;
    }
    public static void main(String[] args) throws IOException {
        String text = "大连大学 [进入旧版网站] English  首 页 学校概况 机构设置 人才培养 科学研究 学科建设 师资队伍 大学文化 招生就业 诚聘英才 公共服务 连大简介 历史沿革 学校领导 校徽校训 校园风光 研究生教育 本科生教育 成人教育 留学生教育 科研管理 科研机构 招生就业指导 中外合作办学 学校邮箱 网络服务 分类导航 快速链接 校园内网 党务公开 校务公开 图书馆 学生园地 协同办公 学生 教工 考生 校友 在线搜索 校长信箱 信息公开 I连大 官方微信 新浪微博 腾讯微博 综合新闻 学校公告 校团委举办团委机关开放日活动 建工学院学生在全国建筑信息模型应用技能大... 我校获“创青春”辽宁省大学生创业大赛金奖... 我校召开理工医学科创新创业工作研讨会... 我校主办当代中国马克思主义文艺批评的理论... 我校2016届毕业生歌会暨校园十大歌手决赛圆... 宋协毅副校长应邀到筑波大学讲学 我校承办第三届全国高校物联网应用创新大赛... 校园网计费通知 关于召开大连大学庆祝中国共产党成立95周年... 2016年“东三省数";
        String query = "华中科技大学";
        HighLighter h1 = new HighLighter();
        SmartChineseAnalyzer analyzer = new SmartChineseAnalyzer();
        String end = h1.HighLight(query, text, analyzer);
        System.out.println(end);
    }
}
