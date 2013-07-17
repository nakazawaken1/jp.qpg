public class Ping {
    /**
     * 使用例  java -cp . Ping 192.168.200.73 3
     * リターンコード  0:PING成功, 1:PING失敗, 2:パラメータ異常
     * @param args [0]IPアドレス [1]タイムアウト(秒)
     */
    public static void main(String[] args) {
        int result = 2;
        if(args.length < 2) {
            System.out.println("使い方 java -cp . Ping [IPアドレス] [タイムアウト(秒)]");
        } else try {
            result = java.net.InetAddress.getByName(args[0]).isReachable(Integer.parseInt(args[1]) * 1000) ? 0 : 1;
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        System.out.println(result);
        System.exit(result);
    }

}
