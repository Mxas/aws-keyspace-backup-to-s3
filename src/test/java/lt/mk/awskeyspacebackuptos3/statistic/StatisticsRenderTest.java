package lt.mk.awskeyspacebackuptos3.statistic;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

class StatisticsRenderTest {


    StatisticsRender statisticsRender;


    private void singleProvider() {
        statisticsRender = new StatisticsRender();
        addFirst();
    }

    private void addFirst() {
        statisticsRender.add(() -> new StatProvider() {
            @Override
            public String h1() {
                return "My First-Try";
            }

            @Override
            public String[] h2() {
                return new String[]{"One", "Second big", "Thrd"};
            }

            @Override
            public String[] data() {
                return new String[]{
                        "true", "25", "156786.5"
                };
            }

            @Override
            public boolean on() {
                return true;
            }
        });
    }


    @Test
    void header() {
        singleProvider();
        String expt = "" +
                "|------------------------|\n" +
                "|      My First-Try      |\n" +
                "|------------------------|\n" +
                "|One |Second big|  Thrd  |\n" +
                "|------------------------|" +
                "";
        Assert.assertEquals(expt, statisticsRender.header());
    }

    @Test
    void data() {
        singleProvider();
        String expt = "" +
//              "|One |Second big|Thrd    |\n" +
                "|true|        25|156786.5|" +
                "";
        Assert.assertEquals(expt, statisticsRender.data());
    }
    private void twoProviders() {
        statisticsRender = new StatisticsRender();
        addFirst();
        statisticsRender.add(() -> new StatProvider() {
            @Override
            public String h1() {
                return "My Second test";
            }

            @Override
            public String[] h2() {
                return new String[]{"One", "Two", "Three", "Four"};
            }

            @Override
            public String[] data() {
                return new String[]{
                        "1", "2", "3", "4"
                };
            }

            @Override
            public boolean on() {
                return true;
            }
        });
    }

    @Test
    void header2() {
        twoProviders();
        String expt = "" +
                "|-------------------------------------------|\n" +
                "|      My First-Try      |  My Second test  |\n" +
                "|-------------------------------------------|\n" +
                "|One |Second big|  Thrd  |One|Two|Three|Four|\n" +
                "|-------------------------------------------|" +
                "";
        Assert.assertEquals(expt, statisticsRender.header());
    }

    @Test
    void data2() {
        twoProviders();
        String expt = "" +
//              "|One |Second big|Thrd    |One|Two|Three|Four|\n" +
                "|true|        25|156786.5|  1|  2|    3|   4|" +
                "";
        Assert.assertEquals(expt, statisticsRender.data());
    }
}