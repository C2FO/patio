var codeView = {

    MENU_HIEGHT_MULTIPLIER:.8,

    CODE_RECIPE:"vim-js.js",

    init:function () {
        ChiliBook.recipes[ codeView.CODE_RECIPE ] =
        {
            _name:'js', _case:true, _main:{
            ml_comment:{
                _match:/\/\*[^*]*\*+(?:[^\/][^*]*\*+)*\//, _style:'color: #808080;'
            }, sl_comment:{
                _match:/\/\/.*/, _style:'color: #ba9357;'
            }, string:{
                _match:/(?:\'[^\'\\\n]*(?:\\.[^\'\\\n]*)*\')|(?:\"[^\"\\\n]*(?:\\.[^\"\\\n]*)*\")/, _style:'color: #a5c261;'
            }, num:{
                _match:/\b[+-]?(?:\d*\.?\d+|\d+\.?\d*)(?:[eE][+-]?\d+)?\b/, _style:'color: #a5c261;'
            }, reg_not:{ //this prevents "a / b / c" to be interpreted as a reg_exp
                _match:/(?:\w+\s*)\/[^\/\\\n]*(?:\\.[^\/\\\n]*)*\/[gim]*(?:\s*\w+)/, _replace:function (all) {
                    return this.x(all, '//num');
                }
            }, reg_exp:{
                _match:/\/[^\/\\\n]*(?:\\.[^\/\\\n]*)*\/[gim]*/, _style:'color: #a5c261;'
            }, brace:{
                _match:/[\{\}]/, _style:'color: deepskyblue; font-weight: bold;'
            }, statement:{
                _match:/\b(with|while|var|try|throw|switch|return|if|for|finally|else|do|default|continue|const|catch|case|break)\b/, _style:'color: #ff6bcf; font-weight: bold;'
            }, error:{
                _match:/\b(URIError|TypeError|SyntaxError|ReferenceError|RangeError|EvalError|Error)\b/, _style:'color: Coral;'
            }, object:{
                _match:/\b(String|RegExp|Object|Number|Math|Function|Date|Boolean|Array)\b/, _style:'color: DeepPink;'
            }, property:{
                _match:/\b(undefined|arguments|NaN|Infinity)\b/, _style:'color: #00cfcf; font-weight: bold;'
            }, 'function':{
                _match:/\b(parseInt|parseFloat|isNaN|isFinite|eval|encodeURIComponent|encodeURI|decodeURIComponent|decodeURI)\b/, _style:'color: olive;'
            }, operator:{
                _match:/\b(void|typeof|this|new|instanceof|in|function|delete)\b/, _style:'color: #cc7833; font-weight: bold;'
            }, liveconnect:{
                _match:/\b(sun|netscape|java|Packages|JavaPackage|JavaObject|JavaClass|JavaArray|JSObject|JSException)\b/, _style:'text-decoration: overline;'
            }
        }
        }
        codeView.setUpCodeView();
        codeView.initMenus();
        $(window).resize(codeView.resize);
    },

    initMenus:function () {
        var timeout = null;


        $(".parentContainer").click(function () {
            var elem = $(this).closest(".menuContainerContent");
            var scroll = elem.scrollTop();
            var children = $(this).toggleClass("expanded").parent().children("ul");
            children.toggle(300,function(){
                children.toggleClass("collapsed");
                codeView.resizeMenus();
                elem.scrollTop(scroll);
            });
        });
        $(".parentContainer").first().click();
    },

    resizeMenus:function () {
        var menuContainers = $('.menuContainer').addClass("menuContainer-noHover");
        var h = $(document).height();
        var tHeight = menuContainers.height("auto").height();
        if(tHeight > h){
            menuContainers.height(h * .9);
        }
    },

    setUpCodeView:function () {
        $(".code").removeClass("code").addClass("vim-js").chili();

        $(".methodDetail").each(function () {
            var showing = false;
            var children = $(this).children(".sourceCode").hide();
            if (children.length) {
                children.each(function () {
                    $(this).children(".code").removeClass("code").addClass("vim-js").chili();
                });
                $(this).hover(
                    function () {
                        $(this).toggleClass("methodDetail-hover").removeClass("methodDetail-nohover");
                        $(this).prepend($("<span style='float : right; text-decoration: underline'>" + (showing ? "Hide" : "Show") + " source</span>"));
                    },
                    function () {
                        $(this).toggleClass("methodDetail-hover").addClass("methodDetail-nohover");
                        $(this).find("span:first").remove();
                    }
                ).click(function () {
                        children[showing ? "slideUp" : "slideDown"](100);
                        showing = !showing;
                    });
            }
        });
    },


    resize:function () {
        codeView.resizeMenus();
    },

    resizeMenu:function () {
        var h = $(document).height();
        var classMenuH = $("#menuContainer").height();
        var memberMenu = $(".classMemberMenu");
        if (memberMenu) {
            var memberHeight = memberMenu.height();
            memberMenu.css("top", classMenuH + 50);
            h = h - (classMenuH + 50);
            if (memberHeight > (h)) {
                memberMenu.height(h * codeView.MENU_HIEGHT_MULTIPLIER);
            }

        }
    }


};

$(document).ready(codeView.init);


