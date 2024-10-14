package com.xiaojukeji.know.streaming.km.common.bean.dto.cluster;

import com.xiaojukeji.know.streaming.km.common.bean.dto.pagination.PaginationSortDTO;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * @author wyb
 * @date 2022/10/17
 */
@Data
public class ClusterGroupSummaryDTO extends PaginationSortDTO {
    @ApiModelProperty("查找该Topic")
    private String searchTopicName;

    @ApiModelProperty("查找该Group")
    private String searchGroupName;
}
