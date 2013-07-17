public class Run {

    /**
     * Usage: java -cp . Run ping -c 2 -w 2 192.168.200.73
     * @param args command
     */
    public static void main(String[] args) {
        if(args.length <= 0) {
            System.out.println("Usage: java -cp . Run [command]\nExample: java -cp . Run ping -c 2 -w 2 192.168.200.73");
        } else try {
            final Process p = new ProcessBuilder(args).redirectErrorStream(true).start();
            new Thread(new Runnable() {
                public void run() {
                    java.util.Scanner scanner = new java.util.Scanner(p.getInputStream());
                    while (scanner.hasNextLine()) {
                        System.out.println(scanner.nextLine());
                    }
                    scanner.close();
                }
            }).start();
            p.waitFor();
            System.out.println(p.exitValue());
        } catch (java.io.IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
