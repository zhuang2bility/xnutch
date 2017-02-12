function rt() {
	if (document.f.ql.value == "" && document.f.qf.value == ""
			&& document.f.qy.value == "" && document.f.qn.value == "") {
		window.location = "search.html";

		return false;
	}
}

function doSubmit() {
	if (document.f.qand.value == "" && document.f.qnot.value == ""
			&& document.f.qor.value == "" && document.f.qphrase.value == "") {
		location.href = "search.html";
	} else {
		var qstr = "";
		if (document.f.qand.value != "") {
			strs = document.f.qand.value.split(/\s+/);
			for (var i = 0; i < strs.length; i++) {
				if (strs[i] == null || strs[i] == " " || strs[i].length == 0)
					continue;
				qstr = qstr + "+" + strs[i] + " ";
			}
		}

		if (document.f.qor.value != "") {
			strs = document.f.qor.value.split(/\s+/);
			for (var i = 0; i < strs.length; i++) {
				if (strs[i] == null || strs[i] == " " || strs[i].length == 0)
					continue;
				qstr = qstr + strs[i] + " ";
			}
		}

		if (document.f.qphrase.value != "")
			qstr = qstr + " " + "\"" + document.f.qphrase.value + "\" ";

		if (document.f.qnot.value != "") {
			strs = document.f.qnot.value.split(/\s+/);
			for (var i = 0; i < strs.length; i++) {
				if (strs[i] == null || strs[i] == " " || strs[i].length == 0)
					continue;
				qstr = qstr + "-" + strs[i] + " ";
			}
		}

		if (document.f.where.value != null && document.f.where.value != "any")
			qstr = document.f.where.value + ":(" + qstr + ")";

		if (document.f.site.value != null && document.f.site.value != "")
			qstr = qstr + "+" + "site:" + document.f.site.value + " ";

		if (document.f.lang.value != null && document.f.lang.value != "any")
			qstr = qstr + "+" + "lang:" + document.f.lang.value + " ";

		if (document.f.format.value != null && document.f.format.value != "any")
			qstr = qstr + "+" + "type:" + document.f.format.value + " ";

		if (document.f.when.value != null && document.f.when.value != "any") {
			days = document.f.when.value;
			currentTime = new Date();
			currentTimeMills = currentTime.getTime();
			months = currentTime.getMonth() + 1;
			if (months < 10)
				months = "0" + months;
			dates = currentTime.getDate();
			if (dates < 10)
				dates = "0" + dates;
			timeStr = currentTime.getFullYear() + "" + months + "" + dates;

			startTime = currentTimeMills - days * 24 * 3600 * 1000;
			currentTime.setTime(startTime);
			months = currentTime.getMonth() + 1;
			if (months < 10)
				months = "0" + months;
			dates = currentTime.getDate();
			if (dates < 10)
				dates = "0" + dates;
			startStr = currentTime.getFullYear() + "" + months + "" + dates;

			qstr = qstr + " " + "date:[" + startStr + " TO " + timeStr + "] ";
		}

		document.search.query.value = qstr;
		document.search.hitsPerPage.value = document.f.hitsPerPage.value;
		document.search.submit();
	}
}

function BindEnter(obj){
	//使用document.getElementById获取到按钮对象
	var button = document.getElementById('AdvancedSearch');
	if(obj.keyCode == 13){
		button.click();
		obj.returnValue = false;
	}
}