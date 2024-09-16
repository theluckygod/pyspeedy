import numpy as np
import requests
import torch
from beartype.typing import List, Literal

from pyspeedy.common.tqdm import tqdm
from pyspeedy.models.config import config
from pyspeedy.models.embedders.embedder import Embedder


class TEIEmbedder(Embedder):
    def __init__(
        self,
        endpoint: str = config.embedder_endpoint,
        embedding_size: int = config.embedding_size,
    ) -> None:
        super().__init__()
        self.endpoint = endpoint
        self.embedding_size = embedding_size

    def embed(
        self,
        request: Embedder.Request,
        return_format: Literal["list", "np", "pt"] = "list",
        batch_size: int = 16,
        disable_tqdm: bool = False,
    ) -> Embedder.Response:

        texts = request.inputs
        emb: List[List[float]] = []
        with tqdm(
            total=len(texts),
            desc="Processing",
            ncols=100,
            disable=disable_tqdm,
        ) as pbar:
            for i in range(0, len(texts), batch_size):
                batch = texts[i : i + batch_size]
                response = requests.post(self.endpoint, json={"inputs": batch})
                response.raise_for_status()
                emb += response.json()

                pbar.update(len(batch))  # Update the progress bar

        if return_format == "np":
            emb = np.array(emb)
        elif return_format == "pt":
            emb = torch.tensor(emb, device="cpu")
        return Embedder.Response(embedding=emb)

    def get_embedding_size(self) -> int:
        return self.embedding_size


if __name__ == "__main__":

    request = Embedder.Request(
        inputs=[
            "Trường hợp nào người sử dụng đất được sử dụng đất ổn định lâu dài?",
            "2. Tư vấn để lấy lại số tiền đã đặt cọc mua đất ?Xin chào luật sư Tôi muốn được tư vấn / giúp đỡ về luật với nội dung như sau: Gia đình tôi có đặt cọc 1 khoản tiền 200 triệu đồng để mua 1 mảnh đất chưa có GCNQSD đất và chỉ ghi giấy viết tay biên nhận với chủ đất với điều khoản là khi làm được giấy chứng nhận quyền sở dụng đất và công chứng hợp đồng mua bán sang cho tôi thì tôi sẽ thanh toán nốt tiền còn lại của mảnh đất .Tuy nhiên khi thực hiện các bước làm GCNQSDĐ thì mới biết đất đó chưa đủ điều kiện làm GCNQSDĐ do vậy tôi yêu cầu nhận lại tiền nhưng bên bán cho rằng đó là tiền đặt cọc và tôi đã bị mất số tiền đó do không tiến hành giao dịch nữa.Theo luật sư thì tôi có thể lấy lại số tiền đó được không? Tôi cần phải làm gì để lấy lại được số tiền đó ?Xin cám ơn và mong sớm nhận được sự tư vấn.",
            "4. Tư vấn thủ tục khi ký kết hợp đồng góp vốn để mua đất ?Xin chào Luật sư,mong luật sư tư vấn giúp tôi vấn đề như sau: Năm 2011, Tôi có mua một mảnh đất tại khu dân cư An Hạ Huyện Bình Chánh của Công ty T. Công ty cũng in hợp đồng góp vốn và hứa đầu quí I năm 2012 sẽ làm sổ đỏ cho tôi. Tuy nhiên, giờ đã năm 2016 rồi mà vẫn chưa thấy tăm hơi gì. Bây giờ tôi nên làm thế nào để có thể bảo vệ quyền lợi của tôi trước pháp luật ?Rất cảm ơn sự tư vấn.",
            "5. Mua đất khi đất đang thế chấp tại ngân hàng ?Xin chào luật sư. Tôi có mua một mảnh đất 700m2 nhưng hiện tại mảnh đất đó đang thế chấp ngân hàng nên hai bên đã làm biên bản thỏa thuận mua bán. Nội dung khi đến hết hạn phải trả ngân hàng, bên bán phải có trách nhiệm lấy sổ đỏ ra và tách sổ sang tên cho bên mua.Vậy xin hỏi khi quá hạn mà người bán không thanh toán được cho ngân hàng thì xử lý thế nào? Tôi phải làm những gì?Xin cảm ơn!",
            "Khi nào có thể đòi lại tiền đặt cọc mua đất ? Mua đất khi đất đang thế chấp tại ngân hàng ?",
            "Nước Việt cần nhiều những Damo Weaver",
            "Nước Việt cần nhiều những Damo Weaver",
            "Tư vấn về chuyển quyền sử dụng đất khi chưa có sổ đỏ.",
            "Tư vấn về chuyển quyền sử dụng đất khi chưa có sổ đỏ.",
            "Xử lý miếng đất đo đạc thực tế sai so với bản đồ",
            "Mượn đất không trả thì giải quyết như thế nào?",
            "Gia đình tôi có 01 mảnh đất có diện tích 1050m2 khai phá từ năm 1993. Đến năm 1999 nhà nước cấp GCN Quyền SD đất cho gia đình tôi 400m2 đất ở và 300m2 đất vườn. đến năm 2016 Nhà nước có dự án quy hoạch đất của gia đình vào khu bến xe.\nQua kiểm tra đối chiếu với các bản đồ địa chính giao đất thì mảnh đất của gia đình nằm trong đất của công ty cổ phần Giống bò sữa Mộc Châu. do đó gia đình tôi chỉ được bồi thường diện tích đã được cấp GCN quyền sử dụng đất và diện tích còn lại thì không được bồi thường là 350m2 đất nông nghiệp. Tôi xin hỏi gia đình tôi có được bồi thường phần diện tích nằm ngoài GCN là 350m2 hay không?Tôi xin chân thành cám ơn !\xa0Câu hỏi được biên tập từ chuyên mục\xa0tư vấn pháp luật đất đai\xa0của Công ty Luật Minh Khuê>>\xa0Luật sư tư vấn pháp luật đất đai trực tuyến gọi:\xa0 1900.6162",
            "Quy định về việc bồi thường đất nông nghiệp ? ",
            ">>\xa0Luật sư tư vấn thủ tục mua bán, chuyển nhượng đất đai, gọi:  1900.6162",
            "abc",
            "Chuyển nhượng quyền sử dụng đất bằng giấy tờ viết tay có hợp pháp không?",
        ]
    )
    response: Embedder.Response = TEIEmbedder().embed(request)
    print(response.embedding)
